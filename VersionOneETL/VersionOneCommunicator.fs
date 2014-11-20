module VersionOneCommunicator

open System.Xml.Linq
open FSharp.Net
open FSharp.Data

open System.Net
open System.Xml
open System
open System.Net


type AttributeDefinition = {Name:string; Token:string; DisplayName:string; AttributeType:String}

type AssetType = {Name:string; Token:string; DisplayName:string; Abstract:bool; AttributeDefinitions:AttributeDefinition seq}

let GetAttributes (uri:string) =
    let getAttributeValue (element:XElement) (name:string) = element.Attribute(XName.Get(name)).Value

    use data = new WebClient()
    let doc = XDocument.Parse <| data.DownloadString(uri)

    query{
        for asset in doc.Descendants(XName.Get("AssetType")) do 
            let getter = getAttributeValue asset
            select  {
                        Name = getter "name";
                        Token = getter "token";
                        DisplayName = getter "displayname";
                        Abstract = bool.Parse ((getter "abstract"));

                        AttributeDefinitions = query {
                            for attributeDefinition in asset.Descendants(XName.Get("AttributeDefinition")) do
                                let getAtt = getAttributeValue attributeDefinition
                                select {
                                            Name = getAtt "name";
                                            Token = getAtt "token";
                                            DisplayName = getAtt "displayname";
                                            AttributeType = getAtt "attributetype"; 
                                        }
                        }

                    }
        }