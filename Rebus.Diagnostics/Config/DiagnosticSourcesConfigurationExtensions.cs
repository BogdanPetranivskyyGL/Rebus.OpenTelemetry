using Rebus.Diagnostics.Incoming;
using Rebus.Diagnostics.Incoming.Overrides;
using Rebus.Diagnostics.Outgoing;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.Pipeline.Send;
using Rebus.Retry;
using Rebus.Retry.FailFast;
using Rebus.Retry.Info;
using Rebus.Retry.Simple;
using System;
using System.Threading;

namespace Rebus.Config;

public static class DiagnosticSourcesConfigurationExtensions
{
    public static OptionsConfigurer EnableDiagnosticSources(this OptionsConfigurer configurer
        , bool canSupportInMemExceptionInfoFactory = true)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));

        configurer.Register<IExceptionInfoFactory>(c =>
        {
            return new InMemExceptionInfoFactory();
        });

        configurer.Decorate<IErrorHandler>(c =>
        {
            return new DecoratedErrorHandle(
                errorHandler: c.Get<IErrorHandler>()
                , errorTracker: c.Get<IErrorTracker>()
                )
                ;
        });

        configurer.Decorate<IErrorTracker>(c =>
        {
            return new DecoratedErrorTracker(c.Get<IErrorTracker>());
        });

        configurer.Decorate<IPipeline>(c =>
        {
            var pipeline = c.Get<IPipeline>();
            var injector = new PipelineStepInjector(pipeline);
                
            var outgoingStep = new OutgoingDiagnosticsStep();
            injector.OnSend(outgoingStep, PipelineRelativePosition.Before,
                typeof(SendOutgoingMessageStep));


            var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
            
            var incomingStep = new IncomingDiagnosticsStep(rebusLoggerFactory.GetLogger<IncomingDiagnosticsStep>());

            var invokerWrapper = new IncomingDiagnosticsHandlerInvokerWrapper();
            injector.OnReceive(invokerWrapper, PipelineRelativePosition.After, typeof(ActivateHandlersStep));

            var concatenator = new PipelineStepConcatenator(injector);
            concatenator.OnReceive(incomingStep, PipelineAbsolutePosition.Front);
                
            return concatenator;
        });
        return configurer;
    }
}