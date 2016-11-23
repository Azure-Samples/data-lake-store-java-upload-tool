#Azure Data Lake Store Data Transfer Tool
This tool uploads data from on-premises relational database
to Azure Data Lake.

#Pre-requisites
- JVM 1.8 or higher
- Read and write privileges to mutate the files on the file system to maintain state
- A valid Azure subscription
- Azure Data Lake Store account. Follow the instructions at [Get started with Azure Data Lake Store using the Azure Portal](https://azure.microsoft.com/en-us/documentation/articles/data-lake-store-get-started-portal/)
- An Active Directory Application. Follow the instructions at [Create an Active Directory Application](https://azure.microsoft.com/en-us/documentation/articles/data-lake-store-authenticate-using-active-directory/#create-an-active-directory-application)
- A valid database backend and the necessary JDBC drivers installed on the server where this code is executed

#How to use

##Building
I am using scala maven assembly plugin to build fat jar
`mvn clean compile install package`

For developers, please run
`mvn clean compile scalastyle:check install package`
After you run the command, please navigate to target/scalastyle-output.xml
to check style check issues if any.

##Usage
```
Usage: db-to-adls-0.1-jar-with-dependencies.jar [options]

  --help                   prints this usage text
  --clientId <value>       Client Id of the Azure active directory application
  --authTokenEndpoint <value>
                           Authentication Token Endpoint of the Azure active directory application
  --clientKey <value>      Client key for the Azure active directory application
  --accountFQDN <value>    Fully Qualified Domain Name of the Azure data lake account
  --destination <value>    Root of the ADLS folder path into which the files will be uploaded
  --octalPermissions <value>
                           Permissions for the file, as octal digits (For Example, 755)
  --desiredParallelism <value>
                           Desired level of parallelism.This will impact your available network bandwidth and source system resources
  --desiredBufferSize <value>
                           Desired buffer size in megabytes.ADLS,by default, streams 4MB at a time. This will impact your available network bandwidth.
  --logFilePath <value>    Log file path
  --reprocess              Indicates that you want to reprocess the table and/or partition
  --driver <value>         Name of the jdbc driver
  --connectionStringUrl <value>
                           Connection String Url for the database backend
  --username <value>       Username used to connect to the database backend
  --password <value>       Password used to connect to the database backend
  --source table1, table2...
                           Please provide table names that need to be transferred.
  --partitions partition1,partition2...
                           Specific partitions that need to be transferred. Can be used for incremental transfer or in combination with reprocess flag
```
##Example command lines

###Prints the help text
```
java \
    -jar target/target/db-to-adls-0.1-jar-with-dependencies.jar \
    --help
```
###Uploads all the partitions ofthe tables specific into the Azure Data Lake
```
java \
    -jar target/db-to-adls-0.1-jar-with-dependencies.jar \
    --clientId <client Id>\
    --authTokenEndpoint <token endpoint>\
    --clientKey <client key>\
    --accountFQDN <url to the data lake store>\
    --destination /dev/data/\
    --octalPermissions 755\
    --desiredParallelism 4\
    --desiredBufferSize 256\
    --logFilePath ~/db-to-adls\
    --driver <jdbc driver>\
    --connectionStringUrl <connection string>\
    --username <user id>\
    --password <password>\
    --source <tables>\
```