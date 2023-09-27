// TODO!

namespace TKVLeaseManager.Domain
{
    public class InstanceData
    {
        // Learners keep a list of decided values to know when a majority was
        // achieved and reply to the client with the final value

        public InstanceData(int instance)
        {
            this.Instance = instance;
            this.IsPaxosRunning = false;
            
            this.DecidedValue = -1;
            this.WrittenValue = -1;
            this.ReadTimestamp = -1;
            this.WriteTimestamp = -1;
            
            this.DecidedReceived = new List<(int, int)>();
        }

        public int Instance { get; set; }
        
        public bool IsPaxosRunning { get; set; }

        public int DecidedValue { get; set; }

        public int ReadTimestamp { get; set; }

        public int WriteTimestamp { get; set; }

        public int WrittenValue { get; set; }

        public List<(int, int)> DecidedReceived { get; set; }
    }
}