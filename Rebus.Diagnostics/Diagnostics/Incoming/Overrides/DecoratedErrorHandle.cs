using Rebus.Bus;
using Rebus.Diagnostics.Helpers;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Retry;
using Rebus.Retry.Info;
using Rebus.Transport;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Diagnostics.Incoming.Overrides;

internal class DecoratedErrorHandle : IErrorHandler, IInitializable
{
    private readonly IErrorHandler innerErrorHandler;
    private readonly IErrorTracker errorTracker;

    public DecoratedErrorHandle(IErrorHandler errorHandler, IErrorTracker errorTracker)
    {
        innerErrorHandler = errorHandler ?? throw new ArgumentNullException(nameof(errorHandler));
        this.errorTracker = errorTracker ?? throw new ArgumentNullException(nameof(errorTracker));
    }

    public void Initialize()
    {
        if (innerErrorHandler is IInitializable initializable)
        {
            initializable.Initialize();
        }
    }

    public async Task HandlePoisonMessage(TransportMessage transportMessage
        , ITransactionContext transactionContext
        , ExceptionInfo exception)
    {
        using var activity = await StartActivity(transportMessage);

        await innerErrorHandler.HandlePoisonMessage(transportMessage, transactionContext, exception);
    }

    private async ValueTask<Activity?> StartActivity(TransportMessage message)
    {
        Activity? activity = null;
        if (RebusDiagnosticConstants.ActivitySource.HasListeners())
        {
            var parent = Activity.Current;
            var headers = message.Headers;

            var messageType = message.GetMessageType();

            var messageWrapper = new TransportMessageWrapper(message);

            var initialTags = TagHelper.ExtractInitialTags(messageWrapper);
            initialTags.Add("messaging.operation", "handle-poison");

            var activityKind = messageWrapper.GetIntentOption() == Headers.IntentOptions.PublishSubscribe
                ? ActivityKind.Consumer
                : ActivityKind.Server;

            var activityName = $"{messageType} handle-poison";

            var exceptions = await errorTracker.GetExceptions(message.GetMessageId());

            activity = RebusDiagnosticConstants.ActivitySource.StartActivity(activityName
                    , activityKind
                    , parent?.Context ?? default(ActivityContext)
                    , initialTags
                    , links: GetLinks(exceptions)
                    );

            activity?.ApplyBaggageFrom(headers);
        }

        return activity;

        
    }

    static IEnumerable<ActivityLink> GetLinks(IReadOnlyList<ExceptionInfo> exceptions)
    {
        foreach (var info in exceptions)
        {
            if (info is InMemExceptionInfo { Exception: { } ex }
                && ex.Data.Contains(RebusDiagnosticConstants.TraceStateHeaderName)
            )
            {
                var traceParent = ex.Data[RebusDiagnosticConstants.TraceStateHeaderName]?.ToString();
                if (traceParent == null)
                    continue;

                if (!ActivityContext.TryParse(traceParent, traceState: null, out var activityContext))
                    continue;

                yield return new ActivityLink(activityContext);
            }
        }
    }
}
