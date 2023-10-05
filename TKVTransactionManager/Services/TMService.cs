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
        public override Task<StatusResponse> Status(StatusRequest request, ServerCallContext context)
        {
            return Task.FromResult(serverService.Status(request));
        }

        public override Task<TransactionResponse> TxSubmit(TransactionRequest request, ServerCallContext context)
        {
            return Task.FromResult(serverService.TxSubmit(request));
        }
        // TODO: communication
    }
}