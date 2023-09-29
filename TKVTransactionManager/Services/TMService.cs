using Grpc.Core;
using System.Threading.Tasks;
using TKVTransactionManager.Services;
using ClientTransactionManagerProto;

namespace TKVTransactionManager.Services
{
    public class TMService : Client_TransactionManagerService.Client_TransactionManagerServiceBase
    {
        private readonly ServerService serverService;

        public TMService(ServerService serverService)
        {
            this.serverService = serverService;
        }

        // TODO: communication
    }
}