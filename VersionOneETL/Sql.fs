module Sql

open System.Data.SqlClient
open System.Text


type FieldCreationType = 
    | Default
    | Custom
    | TableRelation

type DataType = 
    | Varchar   of string
    | DateTime  of string
    | Bit       of string
    | Float     of string
    | Int       of string
    | SmallInt  of string
    | TinyInt   of string
    | Text      of string
    with 
        member this.Raw () = 
            match this with 
            |Varchar           x -> sprintf "%s" x
            |DateTime          x -> sprintf "%s" x
            |Bit               x -> sprintf "%s" x
            |Float             x -> sprintf "%s" x
            |Int               x -> sprintf "%s" x
            |SmallInt          x -> sprintf "%s" x
            |TinyInt           x -> sprintf "%s" x
            |Text              x -> sprintf "%s" x

        override this.ToString () = 
           let format prntPrm value = 
                                match value with 
                                | x when System.String.IsNullOrWhiteSpace x -> "NULL"
                                | _ as v -> sprintf prntPrm v
           
           let formatString = format "'%s'"
           let formatNumber = format "%s"

           match this with 
           | Varchar x -> (if (x.Length >= 1000) then x.Substring(0, 1000) else x) |> (fun(x)->x.Replace("'", "''")) |> formatString
           | DateTime x ->  formatString x
           | Float x -> 
                        let success, value = System.Single.TryParse x

                        if success || System.String.IsNullOrWhiteSpace (x) then formatNumber x else formatString x

           | Int x | SmallInt x | TinyInt x  -> 
                        let success, value = System.Int32.TryParse x


                        if success || System.String.IsNullOrWhiteSpace (x) then formatNumber x else formatString x 

           | Text x -> formatString <| x.Replace("'", "''")
           | Bit x -> 
                       match x with 
                       | x when x.Equals("true", System.StringComparison.InvariantCultureIgnoreCase) -> "1"
                       | _ -> "0"
                       |> formatNumber
      
        

let ConvertToDataType (vOneType) = 
    match vOneType with
    |   "Date" -> ("DATETIME", DataType.DateTime)
    |   "Boolean" -> ("BIT", DataType.Bit)
    |   "Numeric" -> ("FLOAT", DataType.Float)
    |   "State" | "Id" -> ("INT" , DataType.Int)
    |   "Rank" -> ("INT", DataType.TinyInt)
    |   "Description" ->("VARCHAR (MAX)", DataType.Text)
    |   _ -> ("VARCHAR (1000)", DataType.Varchar)
   

type FieldType = {Name:string; ValueType:string; ValueDataTypeFunction: string -> DataType; CustomType:FieldCreationType}
type DataPoint = {Name:string; Value:DataType; ExternalRelation:bool}

type Table = {Name:string; Fields:FieldType seq; CustomTable:bool}

let DropTable (table:string) = 
    sprintf @" 
        IF EXISTS(SELECT * FROM sys.Tables WHERE Name = '%s')
        BEGIN
            Drop TABLE %s
        END" table table
     
let CreateTable (table:Table) = 
    let sb = new StringBuilder()
    let fields = table.Fields
    let tableName = table.Name

    sb.AppendLine (sprintf "CREATE TABLE [%s]" tableName) |> ignore
    sb.AppendLine ("(") |> ignore

    let builder = new StringBuilder ()

    let idType, fnConvert = ConvertToDataType("Id");

    let columnSql = fields  |> Seq.append [{Name = table.Name + "Id"; ValueType=idType; ValueDataTypeFunction=fnConvert; CustomType = FieldCreationType.Custom }]
                            |> Seq.map  (fun(x) -> sprintf "\t[%s] %s" x.Name (x.ValueType.ToString())) 
                            |> String.concat ", \n"
        
    sb.AppendLine(columnSql) |> ignore

    sb.AppendLine(")") |> ignore

    sb.ToString()


    
let CreateInsertStatement (table:string) (points: DataPoint seq) = 

    let buildInsertStatement (table:string) (points: DataPoint seq) = 
        let tableInsSql = sprintf "INSERT INTO %s \n" table 
        let fields = points |> Seq.map(fun(x) -> sprintf "[%s]" x.Name) |> String.concat(",\n") |> (fun(x) -> sprintf "(%s)" x)
        let values = points |> Seq.map(fun(x) -> x.Value.ToString() + "/*" + x.Name + "*/") |> String.concat(",\n") |> (fun(x)->sprintf "(%s)" x)
        tableInsSql + fields + "\nVALUES\n" + values


    let nonRelationDataPoints = points |> Seq.filter(fun(x)->not x.ExternalRelation) |> Seq.toList
    let externalDataPoints = points |> Seq.filter(fun(x)->x.ExternalRelation) |> Seq.toList
    

    let pkIdentifier = points |> Seq.find(fun(x) -> x.Name = table + "Id") |> (fun(x)->x.Value)
    
    let vOneTableInsertStatement = buildInsertStatement table nonRelationDataPoints

    let expandedPoints = 
        seq {
                for dpoint in externalDataPoints do 
                    for value in dpoint.Value.Raw().Split(Constants.ConcatMultipleValues) do 
                        yield {
                                Name = dpoint.Name
                                Value = DataType.Int value
                                ExternalRelation = false
                            }
            } |> Seq.toList
    
    let buildCustomInserts =      expandedPoints |> Seq.map(
                                                            fun(x) -> (
                                                                        sprintf "%s_%s" table x.Name , 
                                                                        seq {
                                                                        
                                                                                //creating a new pair for relation table
                                                                                //Id from relation table row and id from the multiple values.
                                                                                yield {
                                                                                    Name = sprintf "%s_%sId" table x.Name
                                                                                    Value = x.Value
                                                                                    ExternalRelation = false
                                                                                }

                                                                                yield {
                                                                                    Name = sprintf "%sId" table
                                                                                    Value = pkIdentifier
                                                                                    ExternalRelation = false

                                                                                }


                                                                            }

                                                                      )
                                                            )
                                                |> Seq.map(fun(x) -> 
                                                            let table, points = x
                                                            buildInsertStatement table points

                                                    )
    seq {
        yield vOneTableInsertStatement
        yield! buildCustomInserts
    }
    


let CreateTableScripts (types:Table seq) = types |> Seq.map(fun(x) -> CreateTable x)

let CreateDropTableScripts (types:Table seq) = types |> Seq.map (fun(x)-> DropTable x.Name)

