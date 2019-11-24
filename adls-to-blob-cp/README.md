#Purpose
The purpose of this tool is to copy the files from Azure Data Lake Store to Azure Blob Store. There are several ways to accomplish this:
- ADLCopy (but only from blob to Azure data lake)
- DISTCP if you have HDInsight cluster
- ADF

However, all these are closely coupled with Azure Services. And you need to do additional work to enable security, parallelism features. 
In most cases, you will be using some kind of open source platform such as AirFlow or Azkaban and this tool is to enable such a capability.
This tool enables parallel copy of files from ADLS to Blob Store and then notifies via event Hub. The tool is also opinionated and 
shares the file that is secured through Shared Access Signature token. SAS token is at a service level for each file and has a 
time to live attached to the token. 

#Pre-requisites
- JVM 1.8 or higher
- Read and write privileges to mutate the files on the file system to maintain state
- A valid Azure subscription
- Azure Data Lake Store account. Follow the instructions at [Get started with Azure Data Lake Store using the Azure Portal](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-get-started-portal)
- Azure Blob Storage account.
- An Active Directory Application. Follow the instructions at [Create an Active Directory Application](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-authenticate-using-active-directory#create-an-active-directory-application)

__PLEASE NOTE__: You need to install additional jar's that you download from Oracle. The jar's have to be copied to the jre/lib/security folder (as an example
jdk1.8.0_112.jdk/Contents/Home/jre/lib/security). The reason is explained in the following blog posts:
- https://stackoverflow.com/questions/6481627/java-security-illegal-key-size-or-default-parameters
- https://stackoverflow.com/questions/23884990/include-local-policy-and-us-export-policy-jce-unlimited-strength 

#How to use

##Building 
I am using scala sbt assembly plugin to build fat jar. Use the following command to build:
```sbtshell
sbt clean compile assembly
```

##Usage
```sbtshell
Options:


      --adlsFQDN  <arg>                   Fully Qualified Domain Name of the
                                          Azure data lake account
      --blobStoreAccountKey  <arg>        Key to access the blob storage account
                                          (destination)
      --blobStoreAccountName  <arg>       Name of the blob storage account
                                          (destination)
      --blobStoreContainerName  <arg>     Name of the blob store container
                                          (destination)
      --blobStoreRootFolder  <arg>        Root folder in the blob store
                                          container (destination)
      --desiredBufferSize  <arg>          Desired buffer size in megabytes.This
                                          will impact your available network
                                          bandwidth.
      --keyVaultResourceUri <arg>         Key Vault resource uri
      --desiredParallelism  <arg>         Desired level of parallelism.
                                          This will impact your available network
                                          bandwidth and source system resources
      --eventHubName  <arg>               Name of the event hub
      --eventHubNamespaceName  <arg>      Namespace of the event hub
      --eventHubSASKey  <arg>             Shared Access Signature Key for
                                          accessing the event hub
      --eventHubSASKeyName  <arg>         Shared Access Signature key name for
                                          accessing the event hub
      --sourceFile  <arg>                 File in the Azure Data Lake Store to
                                          copy
      --sourceFolder  <arg>               Folder in the Azure Data Lake Store
                                          that contains the file(s) to copy
      --spnClientId  <arg>                Client Id of the Azure active
                                          directory application
      --spnClientKey  <arg>               Client key for the Azure active
                                          directory application
      --spnOauth2TokenEndpoint  <arg>     Authentication Token Endpoint of the
                                          Azure active directory application
      --tokenExpirationInMinutes  <arg>   Please specify the token expiration
                                          time in minutes
      --help                              Show help message
```

##Example command lines
```sbtshell
java -jar adls-to-blob-cp-assembly-0.1.jar\
  --spnClientId xx\
  --spnClientKey xx\
  --spnOauth2TokenEndpoint xx\
  --adlsFQDN xx\
  --blobStoreAccountName xx\
  --blobStoreAccountKey xx\
  --sourceFile xx\
  --blobStoreContainerName xx\
  --blobStoreRootFolder xx\
  --keyVaultResourceUri xx\
  --tokenExpirationInMinutes 00\
  --eventHubNamespaceName xx\
  --eventHubName xx\
  --eventHubSASKeyName xx\
  --eventHubSASKey xx\
  --desiredParallelism 1\
  --desiredBufferSize 1
```

##Prints the help text
```sbtshell
java \ 
    -jar target/scala-2.12/adls-to-blob-cp-assembly-0.1.jar \
    --help
```