﻿using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Text.RegularExpressions;

namespace Utilities
{
    public struct ProcessInfo
    {
        public string Id { get; }
        public string Type { get; }
        public string? Url { get; }

        public ProcessInfo(string id, string type, string url)
        {
            Id = id;
            Type = type;
            Url = url;
        }

        public ProcessInfo(string id, string type)
        {
            Id = id;
            Type = type;
            Url = null;
        }
    }

    public struct ProcessState
    {
        public bool Crashed { get; }
        public List<string> Suspects { get; } // TODO: idk if this should be diff

        public ProcessState(bool crashed, List<string> suspects)
        {
            this.Crashed = crashed;
            this.Suspects = suspects;
        }
    }

    public struct TKVConfig
    {
        public Dictionary<string, string> Client2TM { get; set; }
        public Dictionary<string, string> TM2LM { get; set; }
        public List<ProcessInfo> TransactionManagers { get; }
        public List<ProcessInfo> LeaseManagers { get; }
        public int NumberOfProcesses { get; }
        public (int, TimeSpan) SlotDetails { get; }

        public Dictionary<string, ProcessState>[] ProcessStates { get; }

        public TKVConfig(Dictionary<string, string> client2TM, Dictionary<string, string> TM2LM, List<ProcessInfo> transactionManagers, List<ProcessInfo> leaseManagers, int numberOfProcesses, int slotDuration, TimeSpan startTime, Dictionary<string, ProcessState>[] processStates)
        {
            this.Client2TM = client2TM;
            this.TM2LM = TM2LM;
            this.TransactionManagers = transactionManagers;
            this.LeaseManagers = leaseManagers;
            this.NumberOfProcesses = numberOfProcesses;
            this.SlotDetails = (slotDuration, startTime);
            this.ProcessStates = processStates;
        }

    }

    public static class Common
    {
        // TODO
        public static string GetSolutionDir()
        {
            return Directory.GetParent(AppDomain.CurrentDomain.BaseDirectory).Parent?.Parent?.Parent?.Parent?.FullName;
        }

        public static TKVConfig ReadConfig()
        {
            string configPath = Path.Join(GetSolutionDir(), "Launcher", "config.txt");
            string[] commands;
            try {
                commands = File.ReadAllLines(configPath); 
            } catch (FileNotFoundException e)
            {
                Console.WriteLine("Config file not found.");
                throw e;
            }

            int slotDuration = -1;
            TimeSpan startTime = new TimeSpan();
            Dictionary<string, ProcessState>[] processStates = null;
            List<ProcessInfo> transactionManagers = new List<ProcessInfo>();
            List<ProcessInfo> leaseManagers = new List<ProcessInfo>();
            List<ProcessInfo> servers = new();
            Dictionary<string, string> client2TM = new(); // lets client know which tm to use
            Dictionary<string, string> tm2LM = new();
            int numberOfSlots = 0;
            int readClients = 0;

            foreach (string command in commands)
            {
                string[] args = command.Split(" ");

                if (args[0].Equals("P") && !args[2].Equals("C"))
                {
                    string processId = args[1];
                    ProcessInfo processInfo = new ProcessInfo(processId, args[2], args[3]);
                    switch (args[2])
                    {
                        case "T":
                            transactionManagers.Add(processInfo);
                            servers.Add(processInfo);
                            break;
                        case "L":
                            leaseManagers.Add(processInfo);
                            servers.Add(processInfo);
                            break;
                        default:
                            Console.WriteLine("Invalid process type.");
                            break;
                    }
                }

                else if (args[0].Equals("P")) // client
                {
                    string processId = args[1];
                    ProcessInfo processInfo = new ProcessInfo(processId, args[2]);
                    if (readClients == transactionManagers.Count)
                    {
                        readClients = 0; // round-robin
                    }
                    client2TM.Add(processId, transactionManagers[readClients++].Id);
                }

                else if (args[0].Equals("T"))
                {
                    string[] time = args[1].Split(":");
                    startTime = new TimeSpan(int.Parse(time[0]), int.Parse(time[1]), int.Parse(time[2]));
                }

                else if (args[0].Equals("D"))
                {
                    slotDuration = int.Parse(args[1]);
                }

                else if (args[0].Equals("S"))
                {
                    numberOfSlots = int.Parse(args[1]);
                    processStates = new Dictionary<string, ProcessState>[numberOfSlots];
                }

                else if (args[0].Equals("F"))
                {
                    if (processStates == null)
                    {
                        // Haven't read the number of slots yet
                        continue;
                    }

                    if (args.Length < 2 + servers.Count) { throw new Exception("Invalid config file."); }

                    int slotId = int.Parse(args[1]);
                    processStates[slotId - 1] = new Dictionary<string, ProcessState>();

                    for (int i=0; i< servers.Count; i++)
                    {
                        switch (args[i+2])
                        {
                            case "N":
                                processStates[slotId - 1].Add(servers[i].Id, new ProcessState(false, new List<string>()));
                                break;
                            case "C":
                                processStates[slotId - 1].Add(servers[i].Id, new ProcessState(true, new List<string>()));
                                break;
                            default:
                                throw new Exception("Invalid config file.");
                        }
                    }

                    Regex rg = new Regex(@"\(([^,]+,[^)]+)\)");
                    MatchCollection matched = rg.Matches(command);
                    foreach (Match match in matched.Cast<Match>())
                    {
                        string[] values = match.Groups[1].Value.Split(",");
                        processStates[slotId - 1].TryGetValue(values[0], out ProcessState state);
                        state.Suspects.Add(values[1]);
                    }
                }
            }
            int key = 0;
            for (int i=0; i<transactionManagers.Count; i++)
            {
                if (key > leaseManagers.Count - 1) { key = 0; }
                tm2LM.Add(transactionManagers[i].Id, leaseManagers[key++].Id);
            }


            return new TKVConfig(client2TM, tm2LM, transactionManagers, leaseManagers, transactionManagers.Count + leaseManagers.Count, slotDuration, startTime, processStates);
        }
    }

}