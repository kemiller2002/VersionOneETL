// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open System.Xml.Linq
open FSharp.Net
open FSharp.Data
open Sql;
open System.Net
open System.Threading.Tasks


let CreateDropAndCreateTableScripts (uri) = 
    let types = VersionOne.GetTypes uri |> Seq.toList
    
    let dropTables = Sql.CreateDropTableScripts types   |> String.concat("\n")
    let createTables = Sql.CreateTableScripts types     |> String.concat("\n")

    [dropTables;createTables]

let FieldsToExclude (table:string) = 
    match table with 
        | "Story" -> ["Jeopardy"]
        | _ -> ["Viewers"]

let GetInsertStatements (dataUri:string) (types:Table seq) (createInsertStatement:string -> DataPoint seq -> string seq) = 
    
    seq{ 
        for tp in types |> Seq.filter(fun(x)->not x.CustomTable) do 

            yield seq{

                    let fieldsToExclude = FieldsToExclude(tp.Name)

                    let fieldSelect = tp.Fields 
                                        |> Seq.filter(fun(x) -> not (x.CustomType = FieldCreationType.Custom) )
                                        |> Seq.map(fun(x) -> x.Name) 
                                        |> Seq.filter(fun(x) -> fieldsToExclude |> List.exists (fun(y) -> y = x) = false)  
                                        |> String.concat ","

                    let getDataFn = fun (f) (baseUri:string) (select:string) (noPerPage) (pageNo:int) -> 
                                    let uri = sprintf "%s?sel=%s&page=%i,%i" baseUri select noPerPage pageNo
                                    sprintf "%s\n" uri |> System.Console.WriteLine 
                                    f(uri)
                    
                    let baseUri = sprintf "%s%s" dataUri tp.Name
                    let preppedDatafn = getDataFn VersionOne.GetData baseUri fieldSelect

                    let assets = VersionOne.GetAllAssets preppedDatafn //|> Seq.toList

                    for asset in assets do 
                    
                        //printfn "%s" asset.Name
                        let attributes = asset.Attributes
                        let tp = types |> Seq.find (fun x ->  x.Name = asset.Name)
                        
                        yield attributes      |> Seq.map(fun x ->
//                                                                    printfn "\t\t %s" x.Name
                                                                    let fieldConversion = tp.Fields     |> Seq.tryFind(fun(y) ->y.Name = x.Name) 
                                                                                                |> (
                                                                                                        fun(y)-> 
                                                                                                            match y with 
                                                                                                            | None -> None
                                                                                                            | _ as o -> Some(o.Value.ValueDataTypeFunction)
                                                                                                   
                                                                                                   )
                                                                    match fieldConversion with 
                                                                    |   None -> None
                                                                    |   _ as fieldConversion ->  Some( 
                                                                                                        {
                                                                                                            Name = x.Name
                                                                                                            Value = fieldConversion.Value x.Value
                                                                                                            ExternalRelation = 
                                                                                                                x.CustomGenerated = FieldCreationType.TableRelation 
                                                                                                        }
                                                                                )
                        
                                                    )   |> Seq.filter(fun(y) -> y.IsSome) 
                                                        |> Seq.map(fun(y) -> y.Value)
                                                        |> Seq.append [{
                                                                            Name = sprintf "%sId" asset.Name
                                                                            Value = DataType.Int <| asset.Id
                                                                            ExternalRelation = false
                                                                       }]
                                                        |> (fun x -> createInsertStatement asset.Name x)
            }
    }


let CreateTables (dbConnection:System.Data.SqlClient.SqlConnection) (scripts:string seq) = 
    let scTransaction = dbConnection.BeginTransaction("Tables")

    let dbExecute = DBInteraction.ExecuteCommand dbConnection scTransaction
   
    for script in scripts do 
        dbExecute script |> ignore 

    scTransaction.Commit()

let ProcessStatements (dbConnectionString:string) (statements:string seq seq seq) = 
    let exQueue = new System.Collections.Concurrent.ConcurrentQueue<System.Exception>()

    let complete = Parallel.ForEach(statements, fun (x) (loopState:System.Threading.Tasks.ParallelLoopState) ->   
                                                try
                                                    use indDbConnection = dbConnectionString |> DBInteraction.GetConnection 
                                                    
                                                    let transaction = indDbConnection.BeginTransaction("Insert")
                                                    
                                                    let dbExecute = DBInteraction.ExecuteCommand indDbConnection transaction

                                                    let enqueue = fun(z) ->try dbExecute z |> ignore
                                                                           with | ex -> exQueue.Enqueue <| new System.Exception (z)
                                                                                        raise ex     

                                                    x |> Seq.collect(fun y -> y) |> Seq.iter(enqueue)

                                                    transaction.Commit()
                                                with   
                                                   | ex ->  //System.IO.File.AppendAllText ("C:\\temp\\LoadVOne.log", ex.Message)
                                                            printfn "%s" ex.Message
                                                            exQueue.Enqueue ex
                                                            loopState.Stop()

                               ) |> (fun(x) -> x.IsCompleted)
    (complete, exQueue |> Seq.map(fun(x) -> sprintf "%s:%s" x.Message x.StackTrace))



let LogLoad (dbConnection:System.Data.SqlClient.SqlConnection) = 
    let start = System.DateTime.Now
    
    let completeTransaction () = true
        //let sql = sprintf "UPDATE DataLoad SET WebServiceLoadEndDate = '%A' WHERE LoadDate = '%A'" System.DateTime.Now start
        //let transaction = dbConnection.BeginTransaction("LogTransaction")
        //DBInteraction.ExecuteCommand dbConnection transaction sql |> ignore
        //transaction.Commit()
    

    let runLoad = DBInteraction.ExecuteScalar dbConnection 
                    <| sprintf "SELECT COUNT(*) FROM PAN.DataLoad WHERE LoadDate = '%A' AND WebServiceLoadEndDate IS NOT NULL" start
                    :?> int
    
    (
        match runLoad with 
        | x when x > 0 -> false
        | _ -> 
            let deleteSql = sprintf "DELETE FROM PAN.DataLoad WHERE LoadDate = '%A' AND WebServiceLoadEndDate IS NULL" start
            let transaction = dbConnection.BeginTransaction("LogTransaction")

            if DBInteraction.ExecuteCommand dbConnection transaction deleteSql <= 1 then
               
                let sql = sprintf "INSERT INTO PAN.DataLoad (LoadDate, WebServiceLoadStartDate) VALUES ('%A', '%A')" start start
                DBInteraction.ExecuteCommand dbConnection transaction sql |> ignore
                transaction.Commit () 
                true
            else
                false
            
        ,completeTransaction
    )
let LogMessage (message) =  
    System.IO.File.AppendAllText ("C:\\temp\\LoadVOne.log",message)
    printfn "%s" message

[<EntryPoint>]
let main argv = 
    printfn "%A" argv
    try
        let dbConnectionString = System.Configuration.ConfigurationManager.AppSettings.["InternalTools.VersionOneReporting.ConnectionString"]

        let vOneMetaUrl =  System.Configuration.ConfigurationManager.AppSettings.["InternalTools.VerionOneMeta.Url"]
        let vOneDataUrl =  System.Configuration.ConfigurationManager.AppSettings.["InternalTools.VerionOneData.Url"]

        let dbConnection = dbConnectionString |> DBInteraction.GetConnection 


        let continueProcessing, loadTracking = LogLoad dbConnection

        if(continueProcessing) then

            CreateDropAndCreateTableScripts vOneMetaUrl |> CreateTables dbConnection


            let types = (VersionOne.GetTypes (vOneMetaUrl)) |> Seq.toList

            let statements = GetInsertStatements vOneDataUrl types CreateInsertStatement

            let success, errors =   statements |> ProcessStatements dbConnectionString 

            if(success) then loadTracking () 
            else 
                let message = System.String.Join(System.Environment.NewLine, errors)
                LogMessage message
        else 
                LogMessage "V1 has already loaded for today.  To Re-run delete todays entry out of database log table"
        System.Environment.Exit(0)
        0
     with 
        | ex -> 
                let rec getExceptionDetails (ex:System.Exception) = 
                    match  ex with
                        | null -> ""
                        | ex -> sprintf "%s \n \t %s \n %s" ex.Message ex.StackTrace (getExceptionDetails ex.InnerException)

                let exceptionDetails = sprintf "%A -> %s" System.DateTime.Now (getExceptionDetails ex)
                LogMessage exceptionDetails
                System.Environment.Exit(1)
                -1
        

   
