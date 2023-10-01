using TransactionManagerTransactionManagerProto;
using TransactionManagerLeaseManagerServiceProto;
using System.Data;
using System.Globalization;
using ClientTransactionManagerProto;
using System.Security;
using System.Diagnostics;

namespace TKVTransactionManager.Services
{
    public class ServerService
    {
        // Config file variables
        private readonly string processId;
        private readonly List<bool> processesCrashedPerSlot;
        private readonly List<Dictionary<int, bool>> processesSuspectedPerSlot;
        private readonly Dictionary<string, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers;
        private readonly Dictionary<string, CompareAndSwap.CompareAndSwapClient> leaseManagers; // TODO: fix the service / see if its correct

        // Paxos variables
        private bool isCrashed;
        private int totalSlots;   // The number of total slots elapsed since the beginning of the program
        private int currentSlot;  // The number of experienced slots (process may be frozen and not experience all slots)
        private readonly Dictionary<int, int> primaryPerSlot;

        // Replication variables
        private decimal balance;
        private bool isCleaning;
        private int currentSequenceNumber;
        private List<DADInt> transactionManagerDadInts;
        //private readonly Dictionary<(int, int), ClientCommand> tentativeCommands; // key: (clientId, clientSequenceNumber)
        //private readonly Dictionary<(int, int), ClientCommand> committedCommands;

        public ServerService(
            string processId,
            //List<bool> processCrashedPerSlot,
            //List<Dictionary<int, bool>> processesSuspectedPerSlot,
            Dictionary<string, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers,
            Dictionary<string, CompareAndSwap.CompareAndSwapClient> leaseManagers
            )
        {
            this.processId = processId;
            this.transactionManagers = transactionManagers;
            this.leaseManagers = leaseManagers;
            //this.processCrashedPerSlot = processCrashedPerSlot;
            //this.processesSuspectedPerSlot = processesSuspectedPerSlot;

            this.isCrashed = false;
            this.totalSlots = 0;
            this.currentSlot = 0;
            this.currentSequenceNumber = 0;
            this.transactionManagerDadInts = new List<DADInt>();
            this.primaryPerSlot = new Dictionary<int, int>();

            this.isCleaning = false;
            //this.tentativeCommands = new Dictionary<(int, int), ClientCommand>(); // TODO: client commands
            //this.committedCommands = new Dictionary<(int, int), ClientCommand>();
        }
        // TODO : etc...

        public void PrepareSlot()
        {
            // TODO
        }

        public StatusResponse Status(StatusRequest statusRequest)
        {
            return new StatusResponse { Status = true };
        }

        public TransactionResponse TxSubmit(TransactionRequest transactionRequest)
        {
            List<string> leasesRequired = new List<string>();
            List<DADInt> dadIntsRead = new List<DADInt>();
            Console.WriteLine($"Received transaction request: ");
            Console.WriteLine($"     FROM: {transactionRequest.Id}");
            foreach (string dadintKey in transactionRequest.Reads)
            {
                Console.WriteLine($"     DADINT2READ: {dadintKey}");
                // add to leasesRequired
                leasesRequired.Add(dadintKey);
            }
            foreach (DADInt dadint in transactionRequest.Writes)
            {
                Console.WriteLine($"     DADINT2RWRITE: {dadint.Key}:{dadint.Value}");
                leasesRequired.Add(dadint.Key);
            }

            foreach (string dadint in leasesRequired)
            {
                LeaseRequest leaseRequest = new LeaseRequest { Slot = currentSlot, Lease = { id = processId, permissions = leasesRequired } };
            }

            LeaseResponse leaseResponse = new LeaseResponse(); // ??? leaseManagers[processId].Lease(leaseRequest);
            if (leaseResponse.Status)
            {
                Console.WriteLine($"Lease granted!");
                foreach (string dadintKey in transactionRequest.Reads)
                {
                    dadIntsRead.Add(transactionManagerDadInts.Find(dadint => dadint.Key.Equals(dadintKey)));
                }
                foreach (DADInt dadint in transactionRequest.Writes)
                {
                    DADInt dadintToWrite = transactionManagerDadInts.Find(dadint2 => dadint2.Key.Equals(dadint.Key));
                    dadintToWrite.Value = dadint.Value;
                }
            }

            Console.WriteLine($"Finished processing transaction request...");
            TransactionResponse transactionResponse = new TransactionResponse();
            transactionResponse.Response.AddRange(dadIntsRead);
            return transactionResponse;
        }
    }
}