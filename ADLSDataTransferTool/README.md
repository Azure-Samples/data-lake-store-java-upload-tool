#Azure Data Lake Store Data Transfer Tool
This tool uploads files from on-premises servers to Azure Data Lake.

#Pre-requisites
- JVM 1.8 or higher
- Read and write privileges to mutate the files on the file system to maintain state
- A valid Azure subscription
- Azure Data Lake Store account. Follow the instructions at [Get started with Azure Data Lake Store using the Azure Portal](https://azure.microsoft.com/en-us/documentation/articles/data-lake-store-get-started-portal/)
- An Active Directory Application. Follow the instructions at [Create an Active Directory Application](https://azure.microsoft.com/en-us/documentation/articles/data-lake-store-authenticate-using-active-directory/#create-an-active-directory-application)

#How to use

##Building
I am using maven shade plugin to build fat jar
`mvn clean compile package`

For developers, please run
`mvn clean compile site`
After you run the command, please navigate to target/site/project-summary.html and
navigate the reports. I am using google_styles for checkstyle validation. Please feel
free to change to sun_checks or custom checks.

##Usage
```
usage: ADLSDataTransferTool-0.1.jar [-b <desiredBufferSize>] -c <clientId>
       -d <destination> -f <accountFQDN> -k <clientKey> -o
       <octalPermissions> [-p <desiredParallelism>] [-r] -s <source> -t
       <authTokenEndpoint> [-w <wildcard>]
Azure Data Lake Store Data Transfer Tool
 -b,--desiredBufferSize <desiredBufferSize>     Desired buffer size in
                                                megabytes. This will
                                                impact your available
                                                network bandwidth
 -c,--clientId <clientId>                       Client Id of the Azure
                                                active directory
                                                application
 -d,--destination <destination>                 Root of the ADLS folder
                                                path into which the files
                                                will be uploaded
 -f,--accountFQDN <accountFQDN>                 Fully Qualified Domain
                                                Name of the Azure data
                                                lake account
 -k,--clientKey <clientKey>                     Client key for the Azure
                                                active directory
                                                application
 -o,--octalPermissions <octalPermissions>       permissions for the file,
                                                as octal digits (For
                                                Example, 755)
 -p,--desiredParallelism <desiredParallelism>   Desired level of
                                                parallelism. This will
                                                impact your available
                                                network bandwidth
 -r,--reprocess                                 Indicates that you want to
                                                reprocess the files
 -s,--source <source>                           Root of the folder path
                                                that contains the files to
                                                upload
 -t,--authTokenEndpoint <authTokenEndpoint>     Authentication Token
                                                Endpoint of the Azure
                                                active directory
                                                application
 -w,--wildcard <wildcard>                       Regular expression to
                                                upload the files that
                                                match a specific pattern
```
##Example command line

###Prints the help text
```
java \
    -jar target/ADLSDataTransferTool-0.1.jar \
    -h
```
###Uploads all csv files from local folder to Azure Data Lake
```
java \
    -jar target/ADLSDataTransferTool-0.1.jar \
    -p 4 \
    -b 4 \
    -s /Users/gandhi/code/java/data/ \
    -d /dev/data/ \
    -o 740 \
    -w "**/*.csv" \
    -f gslakestore.azuredatalakestore.net \
    -c 81748f9b-124c-416c-9008-c28408763b02 \
    -k qEqVKaO7UOMYPi9AxVzIXHX26HjJlZveVQUotCoZhIo= \
    -t https://login.microsoftonline.com/fb9dfeb9-5261-4b98-88ff-917109fb067f/oauth2/token
```
###Re-processes all the files from local folder and uploads to Azure Data Lake
```
java \
    -jar target/ADLSDataTransferTool-0.1.jar \
    -r \
    -p 4 \
    -b 4 \
    -s /Users/gandhi/code/java/data/ \
    -d /dev/data/ \
    -o 740 \
    -w "**/*.csv" \
    -f gslakestore.azuredatalakestore.net \
    -c 81748f9b-124c-416c-9008-c28408763b02 \
    -k qEqVKaO7UOMYPi9AxVzIXHX26HjJlZveVQUotCoZhIo= \
    -t https://login.microsoftonline.com/fb9dfeb9-5261-4b98-88ff-917109fb067f/oauth2/token
```