module VersionOne

open System.Xml.Linq
open FSharp.Net
open FSharp.Data
open Sql;
open System.Net
open System.Xml
open System



//type Provider = XmlProvider<"http://versionone.pan/VersionOne/meta.v1">



type Attribute = {
    Name:string
    Value:string
    //ValueType:string
    CustomGenerated:FieldCreationType
}

type Asset = {
    Id:string
    Name:string
    Attributes: Attribute seq
    //Relations:Attribute seq
}

type CustomTable = {
   TableName:string
   ParentTable:string
}

let GetData (uri:string) = 
    let username= System.Configuration.ConfigurationManager.AppSettings.["InternalTools.VersionOneETL.AdminUserName"]
    let password = System.Configuration.ConfigurationManager.AppSettings.["InternalTools.VersionOneETL.AdminPassword"]
    use client = new System.Net.WebClient()
   
    client.Credentials <- new NetworkCredential(username,password)

    client.DownloadString uri


let GetAllAssets (fn:int -> int -> string) = //This gets the actual data not schema
    let calculateStartingNumber (paging) (noPerPage) = (paging/noPerPage + 1) * noPerPage

    let rec getAssets (fn) (noPerPage) (paging) =
        seq {
                let xml = fn noPerPage paging
     
                let xDocument = new System.Xml.XmlDocument()
                xDocument.LoadXml xml
                let nodes = xDocument.SelectNodes "Assets/Asset"
                let totalNodes = System.Int32.Parse(xDocument.DocumentElement.Attributes.["total"].Value)
                let numberNodesLeft =  totalNodes - (calculateStartingNumber paging noPerPage)
                
                for node in nodes do
                    let attributes = node.SelectNodes ("Attribute") |> Seq.cast<XmlNode>
                    let relations = node.SelectNodes("Relation") |> Seq.cast<XmlNode> |> Seq.toList
                    
                    let relationsWithMultiplevalues = relations 
                                                        |> Seq.filter (fun(x) -> x.Attributes.["name"].Value.EndsWith "s"  
                                                                                 //&& not <| x.Attributes.["name"].Value.Equals("status", StringComparison.OrdinalIgnoreCase)
                                                                                 ) 
                                                        |> Seq.toList

                    let relationsWithoutMultiplevalues = relations 
                                                        |> Seq.filter (fun(x) -> not <| x.Attributes.["name"].Value.EndsWith "s" 
                                                                                 //|| x.Attributes.["name"].Value.Equals("status", StringComparison.OrdinalIgnoreCase) 
                                                        )

                    let nodeName, nodeId = node.Attributes.["id"].Value.Split(':') |> (fun(x)->(x.[0],x.[1]))

                    yield {
                            Id = nodeId
                            Name = nodeName
                            Attributes = seq {
                                                    yield! attributes  |> Seq.map(fun(x) ->
                                                                                                {
                                                                                                    Attribute.Name =
                                                                                                                match x.Attributes.["name"].Value with 
                                                                                                                | "Status.Name" -> "Status"
                                                                                                                |  _ as name -> name //x.Attributes.["name"].Value;//.Split('.').[0];
                                                                                                    Attribute.Value = x.InnerText;
                                                                                                    CustomGenerated = FieldCreationType.Default;
                                                                                                }
                                                                                )

                                                    //filter out multiple value nodes to handle later in code.
                                                    yield! relationsWithoutMultiplevalues |> Seq.map(fun(x) ->       
                                                                                                let fld = {
                                                                                                    Attribute.Name = x.Attributes.["name"].Value;
                                                                                                    CustomGenerated = FieldCreationType.Default;
                                                                                                    Attribute.Value = 
                                                                                                        match x.FirstChild with 
                                                                                                        | null -> ""
                                                                                                        | _ -> x.FirstChild.Attributes.["idref"].Value.Split(':').[1]
                                                                                                }
                                                                                                fld

                                                                        )
                                                    yield! relationsWithoutMultiplevalues |> Seq.map(fun(x) ->    
                                                                                                {
                                                                                                    Attribute.Name = sprintf "%sLinkedColumn" x.Attributes.["name"].Value;
                                                                                                    CustomGenerated = FieldCreationType.Custom;
                                                                                                    Attribute.Value = 
                                                                                                        match x.FirstChild with 
                                                                                                        | null -> ""
                                                                                                        | _ -> x.FirstChild.Attributes.["idref"].Value.Split(':').[0]
                                                                                                }
                                                                        )

                                                    yield! relationsWithMultiplevalues |> Seq.map (fun(x) ->
                                                                                                
                                                                                                let concatValues = x.ChildNodes  |> Seq.cast<XmlNode> |> Seq.toList |>
                                                                                                                                    List.map(fun(child) ->
                                                                                                                                        let value = 
                                                                                                                                            match child with 
                                                                                                                                            | null -> ""
                                                                                                                                            | _ -> child.Attributes.["idref"].Value.Split(':').[1]
                                                                                                                                        value
                                                                                                                                    ) 
                                                                                                                                    |> List.filter(fun(x) -> not (String.IsNullOrWhiteSpace x))
                                                                                                                                    |> (fun(x) -> 
                                                                                                                                            let compiledString = 
                                                                                                                                                String.Join(Constants.ConcatMultipleValues.ToString(), x)
                                                                                                                                            compiledString
                                                                                                                                            )

                                                                                                {
                                                                                                    Attribute.Name = sprintf "%s" x.Attributes.["name"].Value;
                                                                                                    CustomGenerated = FieldCreationType.TableRelation;
                                                                                                    
                                                                                                    Attribute.Value = concatValues
                                                                                                }
                                                  
                                                                        )
                                                }

                     }
                            
                match numberNodesLeft with 
                        | t when t <= 0 -> ()
                        | _ -> yield! getAssets fn noPerPage (calculateStartingNumber paging noPerPage)
                             
            }


    let recordPageSize = Constants.NumberPerPage
    getAssets fn recordPageSize 0


let GetTypes (uri:string) =

    let assets = VersionOneCommunicator.GetAttributes uri

    let typesToSkip = "BaseAsset,Workitem,PrimaryWorkitem,Viewers,AssetType".Split(',')


    let assetTypes = assets |> Seq.filter(fun(x)-> match x.Name with 
                                                                  |"BaseAsset" | "Workitem" | "PrimaryWorkitem" | "Viewers" | "AssetType" -> false
                                                                  | _ -> true)

    //add stack item and push custom tables to it. 
    let newTableItems = new System.Collections.Generic.List<CustomTable>();

    let customQueue = new System.Collections.Generic.Queue<Table>()

    seq {
    yield! seq { 
            for typ in assetTypes do 
                        let definitions = typ.AttributeDefinitions
                        yield {
                            CustomTable = false;
                            Name = typ.Name;
                            Fields = seq {
                                            for def in definitions do
                                                match def.AttributeType with 
                                                | "Relation" -> 
                                                            if def.Name.EndsWith("s", System.StringComparison.CurrentCultureIgnoreCase) then
                                                                    let newTable = {
                                                                        TableName = def.Name;
                                                                        ParentTable = typ.Name
                                                                    }
                                                                    
                                                                    newTableItems.Add newTable

                                                                    yield {
                                                                            Name = def.Name;  
                                                                            ValueType = "VARCHAR(100)"; 
                                                                            ValueDataTypeFunction = DataType.Int;
                                                                            CustomType = FieldCreationType.TableRelation 
                                                                          }

                                                            else 
                                                                 yield {
                                                                            Name= sprintf "%sLinkedColumn" def.Name;  
                                                                            ValueType = "VARCHAR(100)"; 
                                                                            ValueDataTypeFunction = DataType.Varchar;
                                                                            CustomType = FieldCreationType.Custom  
                                                                       }

                                                                 yield {
                                                                             Name= def.Name;  
                                                                             ValueType = "INT"; 
                                                                             ValueDataTypeFunction = DataType.Int; 
                                                                             CustomType = FieldCreationType.Default   //change custom type to ternary to allow Not custom but don't add to table.
                                                                       }
                                                | _ -> 
                                                                let definedType = if def.Name = "Description" then "Description" else def.AttributeType
                                                                let fType, vdType = ConvertToDataType  definedType

                                                                yield {
                                                                            Name= def.Name;  
                                                                            ValueType = fType; 
                                                                            ValueDataTypeFunction = vdType; 
                                                                            CustomType = FieldCreationType.Default 
                                                                      }
                                         } |> Seq.toList //have to force this to list to force looping at correct time, because of short taken.  Should go back and fix .
                                                        //issue deals with having to add  newTableItems.Add newTable for custom tables.
                            
                        }
             }
         
         
    yield! seq {
                    for newTable in newTableItems do 
                        yield {
                            CustomTable = true;
                            Name = sprintf "%s_%s" newTable.ParentTable newTable.TableName
                            Fields = seq {
                                    yield 
                                            {
                                                Name = sprintf "%sId" newTable.ParentTable;  
                                                ValueType = "INT"; 
                                                ValueDataTypeFunction = DataType.Int;
                                                CustomType = FieldCreationType.Custom  
                                            }
                                    yield 
                                            {
                                                Name = sprintf "%sId" newTable.TableName;  
                                                ValueType = "INT"; 
                                                ValueDataTypeFunction = DataType.Int;
                                                CustomType = FieldCreationType.Custom 
                                            }
                                }
                        }
                    }
          
        }

    