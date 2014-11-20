module DBInteraction

open System.Data.SqlClient

let GetConnection (connectionString:string) = 
    let connection = new SqlConnection(connectionString)
    
    connection.Open()
    connection    

let ExecuteCommand (connection) (transaction:SqlTransaction) (commandText:string) = 
    
    use command = new SqlCommand()

    command.Connection <- connection
    command.CommandText <- commandText
    command.Transaction <- transaction
    command.ExecuteNonQuery()


let ExecuteScalar(connection) (commandText:string) = 
    use command = new SqlCommand()

    command.Connection <- connection
    command.CommandText <- commandText
    command.ExecuteScalar ()