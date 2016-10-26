#Azure Data Lake Store Data Transfer Tool
This tool uploads files from on-premises servers to Azure Data Lake.

#Pre-requisites
- JVM 1.8 or higher

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

##Example command line

###Prints the help text
java \
    -jar target/ADLSDataTransferTool-0.1.jar
    -h

###
