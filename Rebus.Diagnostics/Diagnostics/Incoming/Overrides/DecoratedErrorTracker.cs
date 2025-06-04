using Rebus.Bus;
using Rebus.Diagnostics.Helpers;
using Rebus.Retry;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Diagnostics.Incoming.Overrides;

internal sealed class DecoratedErrorTracker(IErrorTracker innerErrorTracker) 
    : IErrorTracker
    , IInitializable
{
    public void Initialize()
    {
        if (innerErrorTracker is IInitializable initializable)
        {
            initializable.Initialize();
        }
    }

    public Task CleanUp(string messageId)
        => innerErrorTracker.CleanUp(messageId);

    public Task<IReadOnlyList<ExceptionInfo>> GetExceptions(string messageId)
        => innerErrorTracker.GetExceptions(messageId);

    public Task<string> GetFullErrorDescription(string messageId)
        => innerErrorTracker.GetFullErrorDescription(messageId);

    public Task<bool> HasFailedTooManyTimes(string messageId)
        => innerErrorTracker.HasFailedTooManyTimes(messageId);

    public Task MarkAsFinal(string messageId)
        => innerErrorTracker.MarkAsFinal(messageId);

    public Task RegisterError(string messageId, Exception exception)
    {
        var current = Activity.Current;
        if (current != null)
        {
            exception.Data[RebusDiagnosticConstants.TraceStateHeaderName] = current.Id;
            current.SetStatus(ActivityStatusCode.Error);
            current.AddException(exception);
        }

        return innerErrorTracker.RegisterError(messageId, exception);
    }
}
