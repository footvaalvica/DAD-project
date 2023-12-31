﻿using System.Diagnostics;
using Utilities;

namespace Launcher
{
    internal class Program
    {
        static Process StartProcess(string path, string args)
        {
            var processInfo = new ProcessStartInfo();

            processInfo.UseShellExecute = true;
            processInfo.FileName = path;
            processInfo.Arguments = args;
            processInfo.CreateNoWindow = false;
            processInfo.WindowStyle = ProcessWindowStyle.Normal;

            return Process.Start(processInfo);
        }

        static Process CreateProcess(string baseDirectory, TimeSpan starttime, string[] configArgs)
        {
            var clientPath = Path.Combine(baseDirectory, "TKVClient", "bin", "Debug", "net6.0", "TKVClient.exe");
            var transactionManagerPath = Path.Combine(baseDirectory, "TKVTransactionManager", "bin", "Debug", "net6.0", "TKVTransactionManager.exe");
            var leaseManagerPath = Path.Combine(baseDirectory, "TKVLeaseManager", "bin", "Debug", "net6.0", "TKVLeaseManager.exe");

            var id = configArgs[1];
            var processType = configArgs[2];

            switch (processType)
            {
                case "C":
                    var script = configArgs[3];
                    Console.WriteLine("Starting client " + id + " with script " + script);
                    return StartProcess(clientPath, id + " " + script + " " + starttime);
                case "T":
                case "L":
                    var url = configArgs[3].Remove(0, 7);
                    var host = url.Split(":")[0];
                    var port = url.Split(":")[1];

                    Console.WriteLine("Starting " + processType + " " + id + " at " + host + ":" + port);

                    if (processType.Equals("T"))
                    {
                        return StartProcess(transactionManagerPath, id + " " + host + " " + port + " " + starttime);
                    }
                    else
                    {
                        return StartProcess(leaseManagerPath, id + " " + host + " " + port + " " + starttime);
                    }
                default:
                    throw new Exception("Invalid config file");
            }
        }

        static void Main()
        {
            string baseDirectory = Common.GetSolutionDirectory();
            var configPath = Path.Join(baseDirectory, "Launcher", "config.txt");
            TimeSpan starttime = DateTime.Now.TimeOfDay.Add(TimeSpan.FromSeconds(10));

            if (!File.Exists(configPath))
            {
                Console.WriteLine("Config file not found");
                return;
            }

            var createdProcesses = new List<Process>();
            foreach (var line in File.ReadLines(configPath))
            {
                string[] configArgs = line.Split(" ");
                
                if (configArgs[0].Equals("P"))
                {
                    createdProcesses.Add(CreateProcess(baseDirectory, starttime, configArgs));
                }
            }


            // if the enter key is pressed, kill all processes
            Console.WriteLine("Press enter to kill all processes");
            Console.ReadLine();
            foreach (var process in createdProcesses)
            {
                process.Kill();
            }
            // exit the program
            Environment.Exit(0);
        }
    }
} 