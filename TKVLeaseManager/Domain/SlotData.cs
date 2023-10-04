// TODO!

using Google.Protobuf.Collections;
using LeaseManagerLeaseManagerServiceProto;

namespace TKVLeaseManager.Domain
{
    public class SlotData
    {
        // Learners keep a list of decided values to know when a majority was
        // achieved and reply to the client with the final value

        public SlotData(int slot)
        {
            this.Slot = slot;
            this.IsPaxosRunning = false;
            this.DecidedValues = new List<Lease>();
            this.WrittenValues = new List<Lease>();
            this.ReadTimestamp = -1;
            this.WriteTimestamp = -1;
            this.DecidedReceived = new List<(int, List<Lease>)>();
        }

        public int Slot { get; set; }
        
        public bool IsPaxosRunning { get; set; }

        public List<Lease> DecidedValues { get; set; }

        public int ReadTimestamp { get; set; }

        public int WriteTimestamp { get; set; }

        public List<Lease> WrittenValues { get; set; }

        public List<(int, List<Lease>)> DecidedReceived { get; set; }
    }
}