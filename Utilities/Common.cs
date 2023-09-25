using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace Utilities
{
    public struct ProcessInfo
    {
        public int Id { get; }
        public string Type { get; }
        public string Url { get; }

        public ProcessInfo(int id, string type, string url)
        {
            Id = id;
            Type = type;
            Url = url;
        }
    }

    public struct ProcessState
    {
        public bool Crashed { get; }
        public bool Suspected { get; }

        public ProcessState(bool crashed, bool suspected)
        {
            this.Crashed = crashed;
            this.Suspected = suspected;
        }
    }

    public struct TKVTransactionManagerConfig
    {
        public List<ProcessInfo> TransactionManagers { get; }
        public List<ProcessInfo> LeaseManagers { get; }
        public int NumberOfProcesses { get; }
        public (int, TimeSpan) TimeSlot { get; }

        public Dictionary<int, ProcessState>[] ProcessStates { get; }

        public TKVTransactionManagerConfig(List<ProcessInfo> transactionManagers, List<ProcessInfo> leaseManagers, int numberOfProcesses, (int, TimeSpan) timeSlot, Dictionary<int, ProcessState>[] processStates)
        {
            this.TransactionManagers = transactionManagers;
            this.LeaseManagers = leaseManagers;
            this.NumberOfProcesses = numberOfProcesses;
            this.TimeSlot = timeSlot;
            this.ProcessStates = processStates;
        }

    }

    public static class Common
    {
        public static string GetSolutionDir()
        {
            return Directory.GetParent(AppDomain.CurrentDomain.BaseDirectory).Parent?.Parent?.Parent?.Parent ?.FullName;
        }

        public static TKVTransactionManagerConfig ReadConfig()
        {
            string configPath = Path.Join(GetSolutionDir(), "Launcher", "config.txt");
            string[] lines = File.ReadAllLines(configPath);

            int slotDuration = -1;
            TimeSpan startTime =  new TimeSpan();
            Dictionary<int, ProcessState[]> processStates = null;
            List<ProcessInfo> transactionManagers = new List<ProcessInfo>();
            List<ProcessInfo> leaseManagers = new List<ProcessInfo>();

            Regex rg = new Regex(@"(\([^0-9]*\d+[^0-9]*\))");

            foreach (string line in lines)
            {
                string[] args = line.Split(" ");

                if (args[0].Equals("P") && !args[2].Equals("C"))
                {
                    int processId = int.Parse(args[1]);
                    ProcessInfo processInfo = new ProcessInfo(processId, args[2], args[3]);
                    if (args[2].Equals("T"))
                    {
                        transactionManagers.Add(processInfo);
                    }
                    else if (args[2].Equals("L"))
                    {
                        leaseManagers.Add(processInfo);
                    }
                }

                else if (args[0].Equals("T"))
                {
                    string[] time = args[1].Split(":");
                    startTime = new TimeSpan(int.Parse(time[0]), int.Parse(time[1]), int.Parse(time[2]));
                }

                else if (args[0].Equals("D"))
                {
                    int numOfSlots = int.Parse(args[1]);
                    //processStates = new Dictionary<int, ProcessState>[numOfSlots];
                    // TOD: check if this is correct
                }

                else if (args[0].Equals("F"))
                {
                    if (processStates == null)
                    {
                        continue;
                    }

                    MatchCollection matched = rg.Matches(line);
                    int slotId = int.Parse(args[1]);
                   // processStates[slotId - 1] = new Dictionary<int, ProcessState>();

                    foreach (Match match in matched.Cast<Match>())
                    {
                        string[] values = match.Value.Split(",");
                        int processId = int.Parse(values[0].Remove(0, 1));
                        bool Crashed = values[1].Equals(" C");
                        bool Suspected = values[2].Remove(values[2].Length - 1).Equals(" S");
                        // processStates[slotId - 1].Add(processId, new ProcessState(Crashed, Suspected));
                    }
                } 
            }

        }
    }

}   