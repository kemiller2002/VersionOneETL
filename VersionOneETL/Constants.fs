module Constants

let NumberPerPage = System.Int32.Parse 
                        System.Configuration.ConfigurationManager.AppSettings.["InternalTools.VersionOneETL.ProcessResultsPerPage"]


let ConcatMultipleValues = (char) 034