using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Rebus.Bus;
using Rebus.Diagnostics.Helpers;
using Rebus.Diagnostics.Outgoing;
using Rebus.Messages;
using Rebus.Pipeline;

namespace Rebus.Diagnostics.Incoming
{
    [StepDocumentation("Extracts trace from the incoming message and starts an activity for it")]
    public class IncomingDiagnosticsStep : IIncomingStep
    {
        private static readonly DiagnosticSource DiagnosticListener =
            new DiagnosticListener(RebusDiagnosticConstants.ConsumerActivityName);
        private readonly StepMeter _stepMeter;

        public IncomingDiagnosticsStep()
        {
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

        private static Activity? StartActivity(IncomingStepContext context, TransportMessage message)
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
                if (headers.TryGetValue(RebusDiagnosticConstants.TraceIdHeaderName, out var traceId)
                    && headers.TryGetValue(RebusDiagnosticConstants.TraceSpanIdHeaderName, out var spanId)
                    )
                { 
                    if(!headers.TryGetValue(RebusDiagnosticConstants.TraceFlagHeaderName, out var traceFlagStr))
                    {
                        traceFlagStr = "0";
                    }

                    if (!int.TryParse(traceFlagStr, out var traceFlags))
                    {
                        traceFlags = 0;
                    }

                    headers.TryGetValue(RebusDiagnosticConstants.TraceStateHeaderName, out var traceState);

                    try
                    {
                        var activityContext = new ActivityContext(
                            traceId: ActivityTraceId.CreateFromString(traceId.AsSpan())
                            , spanId: ActivitySpanId.CreateFromString(spanId.AsSpan())
                            , traceFlags: (ActivityTraceFlags)traceFlags
                            , traceState: traceState
                            );

                        links = [new ActivityLink(activityContext)];
                    }
                    catch { }
                }

                activity = RebusDiagnosticConstants.ActivitySource.StartActivity(activityName
                        , activityKind
                        , default(ActivityContext)
                        , initialTags
                        , links);

                if (activity != null)
                {
                    CopyBaggage(headers, activity);
                }

                // TODO: Not sure if this is still needed
                // DiagnosticListener.OnActivityImport(activity, context);
            }
            
            SendBeforeProcessEvent(context, activity);

            return activity;
        }

        private static void CopyBaggage(Dictionary<string, string> headers, Activity activity)
        {
            if (headers.TryGetValue(RebusDiagnosticConstants.BaggageHeaderName, out var baggageContent))
            {
                var baggage =
                    JsonConvert.DeserializeObject<IEnumerable<KeyValuePair<string, string>>>(baggageContent);

                foreach (var keyValuePair in baggage)
                {
                    activity.AddBaggage(keyValuePair.Key, keyValuePair.Value);
                }
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