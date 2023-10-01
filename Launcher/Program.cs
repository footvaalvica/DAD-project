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
            var pInfo = new ProcessStartInfo();

            pInfo.UseShellExecute = true;
            pInfo.FileName = path;
            pInfo.Arguments = args;
            pInfo.CreateNoWindow = false;
            pInfo.WindowStyle = ProcessWindowStyle.Normal;

            return Process.Start(pInfo);
        }

        static Process CreateProcess(string baseDir, string[] configArgs)
        {
            var clientPath = Path.Combine(baseDir, "TKVClient", "bin", "Debug", "net6.0", "TKVClient.exe");
            var transactionManagerPath = Path.Combine(baseDir, "TKVTransactionManager", "bin", "Debug", "net6.0", "TKVTransactionManager.exe");
            var leaseManagerPath = Path.Combine(baseDir, "TKVLeaseManager", "bin", "Debug", "net6.0", "TKVLeaseManager.exe");

            var id = configArgs[1];
            var type = configArgs[2];

            if (type.Equals("C"))
            {
                var script = configArgs[3];
                Console.WriteLine("Starting client " + id + " with script " + script);
                return StartProcess(clientPath, id + " " + script);
            }

            else if (type.Equals("T") || type.Equals("L"))
            {
                var url = configArgs[3].Remove(0, 7);
                var host = url.Split(":")[0];
                var port = url.Split(":")[1];

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
            var configPath = Path.Join(baseDir, "Launcher", "config.txt");

            if (!File.Exists(configPath))
            {
                Console.WriteLine("Config file not found");
                return;
            }

            foreach (var line in File.ReadLines(configPath))
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