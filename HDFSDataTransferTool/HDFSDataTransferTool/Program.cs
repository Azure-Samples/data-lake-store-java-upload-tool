using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.IO;
using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace HDFSDataTransferTool
{
    class Program
    {
        private const string listArgs = "?op=LISTSTATUS";
        private const string openArgs = "?&op=OPEN";
        private static int fileCount = 0, transferCount = 0;
        private static string baseURL = "", localBase = null, containerName = "", storageAccountName = "", storageAccountKey = "";
        private static bool deleteOption = false, logToSql = false;
        private static DateTime start, end;
        private static StreamWriter logFileOutput, errorWriter;

        static void Main(string[] args)
        {
            Console.Title = "HDFS Data Transfer Tool";
            string headNode = "";
            string source = ""; 
            Dictionary<string, string> parsedArgs = new Dictionary<string, string>();
            foreach(var arg in args)
            {
                if(arg.Contains(':'))
                {
                    var parsedArg = arg.Split(new char[1] { ':' }, 2);
                    parsedArgs.Add(parsedArg[0], parsedArg[1]);
                }
                else
                {
                    parsedArgs.Add(arg, null);
                }
            }            

            //make sure required arguments exist
            if (parsedArgs.Count < 3 || !parsedArgs.Keys.Contains("/HeadNode") || !parsedArgs.Keys.Contains("/Container") || !parsedArgs.Keys.Contains("/Source") || !parsedArgs.Keys.Contains("/Dest") || !parsedArgs.Keys.Contains("/DestKey"))
            {
                Console.WriteLine("Please enter the required arguments: /HeadNode, /Source, /Dest, /DestKey, and /Container");
                Console.ReadLine();
                Environment.Exit(0);
            }
            if (parsedArgs.Keys.Contains("/HeadNode"))
            {
                headNode = parsedArgs["/HeadNode"];
            }
            if (parsedArgs.Keys.Contains("/DestKey"))
            {
                storageAccountKey = parsedArgs["/DestKey"];
            }
            if (parsedArgs.Keys.Contains("/Source"))
            {
                source = parsedArgs["/Source"];
            }
            if (parsedArgs.Keys.Contains("/Dest"))
            {
                storageAccountName = parsedArgs["/Dest"];
            }
            if (parsedArgs.Keys.Contains("/Container"))
            {
                containerName = parsedArgs["/Container"];
            }
            if (parsedArgs.Keys.Contains("/S"))
            {
                logToSql = true;
            }
            if (parsedArgs.Keys.Contains("/D"))
            {
                deleteOption = true;
            }

            //create base WebHDFS URL
            baseURL = "http://" + headNode + ":50070/webhdfs/v1";

            //Create log and error file direcotories and streamwriters
            string logFilePath = Directory.GetCurrentDirectory() + "\\Logs\\" + DateTime.UtcNow.Year + "\\" + DateTime.UtcNow.Month + "\\" + DateTime.UtcNow.Day + "\\";
            string errorFilePath = Directory.GetCurrentDirectory() + "\\Errors\\" + DateTime.UtcNow.Year + "\\" + DateTime.UtcNow.Month + "\\" + DateTime.UtcNow.Day + "\\";
            Directory.CreateDirectory(logFilePath);
            Directory.CreateDirectory(errorFilePath);
            logFileOutput = new StreamWriter(logFilePath + "Log_" + DateTime.UtcNow.ToString("HH:mm:ss").Replace("/", "-").Replace(" ", "_").Replace(':', '-') + ".log");
            errorWriter = new StreamWriter(errorFilePath + "Error_" + DateTime.UtcNow.ToString("HH:mm:ss").Replace("/", "-").Replace(" ", "_").Replace(':', '-') + ".log");
            start = DateTime.UtcNow;

            //path to the list of directories
            ArrayList directories = GetDirectories(source);
            //Count of files to transfer
            int fileCount = directories.Count;

            //local root directory to save HDFS files
            localBase = Directory.GetCurrentDirectory();
            string basePath = null;
            string localPath = null;
            foreach (String directory in directories)
            {
                basePath = baseURL + directory;
                localPath = localBase + directory.Replace("/", "\\");
                try
                {
                    DownloadFiles(basePath, localPath);
                }
                catch (Exception e)
                {
                    logFileOutput.Close();
                    Console.WriteLine("-----------------");
                    Console.WriteLine(e.Message);
                    Console.WriteLine("Press Enter key to exit...");
                    Console.ReadLine();
                    Environment.Exit(0);
                }
            }

            while(transferCount<fileCount)
            {
                continue;
            }
            ProcessDirectory(localBase);

            end = DateTime.UtcNow;
            var duration = end - start;

            Console.WriteLine("All files downloaded successfully!\nTotal Duration: {0}\nPress Enter key to exit...", duration);
            logFileOutput.Close();
            errorWriter.Close();
            Console.ReadLine();
        }

        //Retuns an ArrayList of HDFS directories
        private static ArrayList GetDirectories(string directoryFilePath)
        {
            ArrayList al = new ArrayList();
            StreamReader directoryReader = new StreamReader(directoryFilePath);
            string line;
            while ((line = directoryReader.ReadLine()) != null && line != "")
            {
                al.Add(line);
            }
            return al;
        }

        //Utilizes WebHDFS API to download files from HDFS, writing to the root directory specified in the command line arguments
        private static void DownloadFiles(string basePath, string localPath)
        {
            try
            {
                using (var client = new WebClient())
                {
                    //Get the result of the WebHDFS call
                    string result = client.DownloadString(basePath + listArgs);
                    JObject o = JObject.Parse(result);

                    int dirCount = o["FileStatuses"]["FileStatus"].Count();
                    if (dirCount > 0)
                    {
                        //for each directory, list the type, suffix, and size of the file
                        for (int i = 0; i < dirCount; i++)
                        {
                            string dirType = (string)o["FileStatuses"]["FileStatus"][i]["type"];
                            string pathSuffix = (string)o["FileStatuses"]["FileStatus"][i]["pathSuffix"];
                            string fileSize = (string)o["FileStatuses"]["FileStatus"][i]["length"];
                            string newBasePath = null;
                            string newLocalPath = null;
                            if (basePath.EndsWith("/"))
                            {
                                newBasePath = basePath + pathSuffix;
                                newLocalPath = localPath + pathSuffix;
                            }
                            else
                            {
                                newBasePath = basePath + "/" + pathSuffix;
                                newLocalPath = localPath + "\\" + pathSuffix;
                            }

                            //if current file status is a file, download the file
                            if (dirType.Equals("FILE"))
                            {
                                String[] fileProperties = new String[6];
                                DateTime fileStart = DateTime.UtcNow;
                                string downloadString = newBasePath + openArgs;
                                Directory.CreateDirectory(localPath);
                                Console.WriteLine("Downloading file {0}\nDounload started: {1}", newBasePath, fileStart);

                                //download the file to the newly created directory
                                client.DownloadFile(downloadString, newLocalPath);
                                var fileEnd = DateTime.UtcNow;
                                var fileDuration = fileEnd - fileStart;

                                //add file properties to an array for output logs/logging to sql server
                                fileProperties.SetValue(DateTime.UtcNow.ToShortDateString(), 0);
                                fileProperties.SetValue(basePath.Replace(baseURL, ""), 1);
                                fileProperties.SetValue(pathSuffix, 2);
                                fileProperties.SetValue(fileStart.ToString(), 3);
                                fileProperties.SetValue(fileEnd.ToString(), 4);
                                fileProperties.SetValue(fileSize, 5);
                                Console.WriteLine("Download ended: {0}\nDuration: {1}\n\n", fileProperties[4], fileDuration);
                                string fileLog = null;
                                for (int index = 0; index < fileProperties.Length; index++)
                                {
                                    if (index == fileProperties.Length - 1)
                                    {
                                        fileLog += fileProperties[index] + "\n\r";
                                    }
                                    else
                                    {
                                        fileLog += fileProperties[index] + "|";
                                    }
                                }
                                logFileOutput.Write(fileLog);
                                logFileOutput.Flush();
                                Console.WriteLine("Uploading file {0}\n\n", newBasePath);
                                try
                                {
                                    UploadToBlobAsync(newLocalPath, newBasePath);
                                }
                                catch(Exception e)
                                {
                                    Console.WriteLine(e.Message);
                                    continue;
                                }
                                
                                if (logToSql.Equals("true"))
                                {
                                    LogToDatabase(fileProperties);
                                }
                            }

                            //if current file status is a directory, recursively call DownloadFiles on the sub directory
                            else if (dirType.Equals("DIRECTORY"))
                            {
                                try
                                {
                                    DownloadFiles(newBasePath, newLocalPath);
                                }
                                catch (Exception e)
                                {
                                    errorWriter.WriteLine("There was an error accessing directory {0}\n\t{1}", newBasePath, e.Message);
                                    errorWriter.Flush();
                                    Console.WriteLine("There was an error accessing directory {0}\n\t{1}", newBasePath, e.Message);
                                    continue;
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (e.Message.Contains("404"))
                {
                    throw e;
                }
                else
                {
                    Console.WriteLine(e.Message);
                    DownloadFiles(basePath, localPath);
                }
            }
        }

        //Upload HDFS files to Azure blob storage
        private static async void UploadToBlobAsync(string localPath, string basePath)
        {
            string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=" + storageAccountName + ";AccountKey=" + storageAccountKey + ";";
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            // Retrieve reference to a blob with the same name and path as the HDFS file.
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(basePath.Replace(baseURL + "/", ""));

            // Create or overwrite the blob with contents from the newly created local file.  
            try
            {
                await blockBlob.UploadFromFileAsync(localPath, FileMode.Open);
            }          
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }
            

            if(deleteOption)
            {
                DeleteCurrentFile(localPath);
            }
            
            transferCount++;
            
            
        }

        //Delete file from the directory
        private static void DeleteCurrentFile(string localPath)
        {
            File.Delete(localPath);
        }

        //Delete file from the directory
        private static void ProcessDirectory(string directoryBase)
        {
            foreach (var directory in Directory.GetDirectories(directoryBase))
            {
                ProcessDirectory(directory);
                if (Directory.GetFiles(directory).Length == 0 &&
                    Directory.GetDirectories(directory).Length == 0)
                {
                    Directory.Delete(directory, false);
                }
            }
            
        }

        //Logs HDFS file download properties to an Azure SQL Database
        private static void LogToDatabase(String[] fileProperties)
        {
            string connectionString = ConfigurationManager.ConnectionStrings["AzureSqlConnectionString"].ToString();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlDataReader queryResult;
                string queryString = "INSERT INTO FileLogs VALUES(";
                for (int index = 0; index < fileProperties.Length; index++)
                {
                    if (index == fileProperties.Length - 1)
                    {
                        queryString += fileProperties[index] + ");";
                    }
                    else
                    {

                        queryString += "'" + fileProperties[index] + "', ";
                    }
                }

                SqlCommand command = new SqlCommand(queryString);
                command.Connection = connection;
                connection.Open();
                queryResult = command.ExecuteReader();
                connection.Close();
            }
        }
    }
}
