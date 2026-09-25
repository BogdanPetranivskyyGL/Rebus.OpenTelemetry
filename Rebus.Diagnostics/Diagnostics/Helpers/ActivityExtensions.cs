using Newtonsoft.Json;
using Rebus.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Rebus.Diagnostics.Helpers;

internal static class ActivityExtensions
{
    
    const string ExceptionEventName = "exception";
    const string ExceptionMessageTag = "exception.message";
    const string ExceptionStackTraceTag = "exception.stacktrace";
    const string ExceptionTypeTag = "exception.type";

    
    // "Borrowed" from https://github.com/tarekgh/runtime/blob/d4464d7ae6d99d75ff89309fb56fd2a5a8f0c845/src/libraries/System.Diagnostics.DiagnosticSource/src/System/Diagnostics/Activity.cs#L535
    // to avoid requiring version 9 of System.Diagnostics.DiagnosticSource
    public static Activity AddException(this Activity activity, Exception exception,
        ActivityTagsCollection? tags = null, DateTimeOffset timestamp = default)
    {
        if (exception == null)
        {
            throw new ArgumentNullException(nameof(exception));
        }

        var exceptionTags = tags ?? new();

        var hasMessage = false;
        var hasStackTrace = false;
        var hasType = false;

        foreach (var pair in exceptionTags)
        {
            if (pair.Key == ExceptionMessageTag)
            {
                hasMessage = true;
            }
            else if (pair.Key == ExceptionStackTraceTag)
            {
                hasStackTrace = true;
            }
            else if (pair.Key == ExceptionTypeTag)
            {
                hasType = true;
            }

            if (hasMessage && hasStackTrace && hasType)
                break;
        }

        if (!hasMessage)
        {
            exceptionTags.Add(new KeyValuePair<string, object?>(ExceptionMessageTag, exception.Message));
        }

        if (!hasStackTrace)
        {
            exceptionTags.Add(new KeyValuePair<string, object?>(ExceptionStackTraceTag, exception.ToString()));
        }

        if (!hasType)
        {
            exceptionTags.Add(new KeyValuePair<string, object?>(ExceptionTypeTag, exception.GetType().ToString()));
        }

        return activity.AddEvent(new ActivityEvent(ExceptionEventName, timestamp, exceptionTags));
    }


    public static void ApplyBaggageFrom(this Activity activity, IReadOnlyDictionary<string, string> headers, ILog? log = null)
    {
        if (!headers.TryGetValue(RebusDiagnosticConstants.BaggageHeaderName, out var baggageContent))
        {
            return;
        }

        try
        {
            var baggage =
                JsonConvert.DeserializeObject<IEnumerable<KeyValuePair<string, string>>>(baggageContent);

            if (baggage == null)
            {
                return;
            }

            foreach (var keyValuePair in baggage)
            {
                activity.AddBaggage(keyValuePair.Key, keyValuePair.Value);
            }
        }
        catch (Exception e)
        {
            log?.Warn(e, "Failed to process activity baggage: {0}", baggageContent);
        }
    }
}