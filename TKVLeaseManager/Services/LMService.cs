using Grpc.Core;
using ClientLeaseManagerProto;
using TKVLeaseManager.Services;

namespace TKVLeaseManager.Services
{
    public class LMService : Client_LeaseManagerService.Client_LeaseManagerServiceBase
    {
        private readonly LeaseManagerService leaseManagerService;

        public LMService(LeaseManagerService leaseManagerService)
        {
            this.leaseManagerService = leaseManagerService;
        }
        public override Task<StatusResponseLM> Status(StatusRequestLM request, ServerCallContext context)
        {
            return Task.FromResult(leaseManagerService.Status(request));
        }
    }
}