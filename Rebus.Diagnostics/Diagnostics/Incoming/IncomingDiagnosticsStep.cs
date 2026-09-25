using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Rebus.Bus;
using Rebus.Diagnostics.Helpers;
using Rebus.Diagnostics.Outgoing;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Pipeline;

namespace Rebus.Diagnostics.Incoming
{
    [StepDocumentation("Extracts trace from the incoming message and starts an activity for it")]
    public class IncomingDiagnosticsStep : IIncomingStep
    {
        private readonly ILog _log;

        private static readonly DiagnosticSource DiagnosticListener =
            new DiagnosticListener(RebusDiagnosticConstants.ConsumerActivityName);
        private readonly StepMeter _stepMeter;

        public IncomingDiagnosticsStep(ILog log)
        {
            _log = log;
            _stepMeter = new StepMeter("incoming");
        }

        public async Task Process(IncomingStepContext context, Func<Task> next)
        {
            var message = context.Load<TransportMessage>();

            using var activity = StartActivity(context, message);

            _stepMeter.Observe(message);

            try
            {
                await next();
            }
            finally
            {
                SendAfterProcessEvent(activity, context);
            }
        }

        private Activity? StartActivity(IncomingStepContext context, TransportMessage message)
        {
            try
            {
                Activity? activity = null;
                if (RebusDiagnosticConstants.ActivitySource.HasListeners())
                {
                    var headers = message.Headers;

                    var messageType = message.GetMessageType();

                    var messageWrapper = new TransportMessageWrapper(message);

                    var initialTags = TagHelper.ExtractInitialTags(messageWrapper);
                    initialTags.Add("messaging.operation", "receive");

                    var activityKind = messageWrapper.GetIntentOption() == Headers.IntentOptions.PublishSubscribe
                        ? ActivityKind.Consumer
                        : ActivityKind.Server;

                    var activityName = $"{messageType} receive";
                    IEnumerable<ActivityLink>? links = null;
                    if (headers.TryGetValue(RebusDiagnosticConstants.TraceStateHeaderName, out var traceId)
                        && traceId is { }
                        && ActivityContext.TryParse(traceId, traceState: null, out var linkContext))
                    {
                        links = [new ActivityLink(linkContext)];
                    }

                    activity = RebusDiagnosticConstants.ActivitySource.StartActivity(activityName
                            , activityKind
                            , default(ActivityContext)
                            , initialTags
                            , links);

                    activity?.ApplyBaggageFrom(headers, _log);

                    // TODO: Not sure if this is still needed
                    // DiagnosticListener.OnActivityImport(activity, context);
                }

                SendBeforeProcessEvent(context, activity);

                return activity;
            }
            catch (Exception e)
            {
                _log.Warn(e, "Failed to start message activity. Continuing without");
                return null;
            }
        }

        private static void SendBeforeProcessEvent(IncomingStepContext context, Activity? activity)
        {
            if (DiagnosticListener.IsEnabled(BeforeProcessMessage.EventName, context))
            {
                DiagnosticListener.Write(BeforeProcessMessage.EventName, new BeforeProcessMessage(context, activity));
            }
        }

        private static void SendAfterProcessEvent(Activity? activity, IncomingStepContext context)
        {
            if (DiagnosticListener.IsEnabled(AfterProcessMessage.EventName))
            {
                DiagnosticListener.Write(AfterProcessMessage.EventName, new AfterProcessMessage(context, activity));
            }
        }
    }
}