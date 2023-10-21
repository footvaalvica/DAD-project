using TransactionManagerTransactionManagerProto;

namespace TKVTransactionManager.Services
{
    public class GossipService : Gossip.GossipBase
    {
        private readonly ServerService serverService;

        public GossipService(ServerService serverService)
        {
            this.serverService = serverService;
        }
    }
}