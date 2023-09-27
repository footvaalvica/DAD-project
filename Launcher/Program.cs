using System;
using System.Diagnostics;
using System.IO;
using System.Security.AccessControl;
using Utilities;

namespace Launcher
{
    internal class Program
    {
        static Process StartProcess(string path, string args)
        {
            ProcessStartInfo pInfo = new ProcessStartInfo();

            pInfo.UseShellExecute = true;
            pInfo.FileName = path;
            pInfo.Arguments = args;
            pInfo.CreateNoWindow = false;
            pInfo.WindowStyle = ProcessWindowStyle.Normal;

            return Process.Start(pInfo);
        }

        static Process CreateProcess(string baseDir, string[] configArgs)
        {
            string clientPath = Path.Combine(baseDir, "TKVClient", "bin", "Debug", "net6.0", "TKVClient.exe");
            string transactionManagerPath = Path.Combine(baseDir, "TKVTransactionManager", "bin", "Debug", "net6.0", "TKVTransactionManager.exe");
            string leaseManagerPath = Path.Combine(baseDir, "TKVLeaseManager", "bin", "Debug", "net6.0", "TKVLeaseManager.exe");

            string id = configArgs[1];
            string type = configArgs[2];

            if (type.Equals("C"))
            {
                string script = configArgs[3];
                return StartProcess(clientPath, id + " " + script);
            }

            else if (type.Equals("T") || type.Equals("L"))
            {
                string url = configArgs[3].Remove(0, 7);
                string host = url.Split(":")[0];
                string port = url.Split(":")[1];

                Console.WriteLine("Starting " + type + " " + id + " at " + host + ":" + port);

                if (type.Equals("T"))
                {
                    return StartProcess(transactionManagerPath, id + " " + host + " " + port);
                }
                else
                {
                    return StartProcess(leaseManagerPath, id + " " + host + " " + port);
                }
            }

            else
            {
                throw new Exception("Invalid config file");
            }
        }

        static void Main()
        {
            string baseDir = Common.GetSolutionDir();
            string configPath = Path.Join(baseDir, "Launcher", "config.txt");

            if (!File.Exists(configPath))
            {
                Console.WriteLine("Config file not found");
                return;
            }

            foreach (string line in File.ReadLines(configPath))
            {
                string[] configArgs = line.Split(" ");
                
                if (configArgs[0].Equals("P"))
                {
                    CreateProcess(baseDir, configArgs);
                }
            }
        }


    }
} 