#Azure Data Lake Store Data Transfer Tool
This tool uploads data from on-premises relational database
to Azure Data Lake.

#Pre-requisites
- JVM 1.8 or higher
- Read and write privileges to mutate the files on the file system to maintain state
- A valid Azure subscription
- Azure Data Lake Store account. Follow the instructions at [Get started with Azure Data Lake Store using the Azure Portal](https://azure.microsoft.com/en-us/documentation/articles/data-lake-store-get-started-portal/)
- An Active Directory Application. Follow the instructions at [Create an Active Directory Application](https://azure.microsoft.com/en-us/documentation/articles/data-lake-store-authenticate-using-active-directory/#create-an-active-directory-application)

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
```
##Example command lines

###Prints the help text
```
java \
    -jar target/target/db-to-adls-0.1-jar-with-dependencies.jar \
    --help
```
###Uploads all csv files from local folder to Azure Data Lake
```
java \
    -jar target/target/db-to-adls-0.1-jar-with-dependencies.jar \
    --clientId 0b83a076-69e2-410e-b6b4-399ff2e63e23\
    --authTokenEndpoint https://login.microsoftonline.com/fb9dfeb9-5261-4b98-88ff-917109fb067f/oauth2/token\
    --clientKey u50xD1RG4GlECn7KlCdW8DEVMEMB+FZmiSisTpI9Ugo=\
    --accountFQDN gslakestore.azuredatalakestore.net/\
    --destination /dev/data/\
    --octalPermissions 755\
    --desiredParallelism 4\
    --desiredBufferSize 256\
    --logFilePath /Users/gaswamin/oss/data-lake-store-java-upload-tool/db-to-adls
```