package org.acme.quartz;

import org.eclipse.microprofile.context.spi.ThreadContextProvider;
import org.eclipse.microprofile.context.spi.ThreadContextSnapshot;
import org.slf4j.MDC;

import java.util.Map;

/*
This ThreadContextProvider is required to get MDCs propagated to aws sdk async threads (e.g. sdk-async-response-1-0)
Essentially required, to get traceId, spanId fields in log entries set
also in logs from mutiny continuation code (.onItem()...).
Direct copy from: https://stackoverflow.com/a/65040601/753724
*/
public class MdcContextProvider implements ThreadContextProvider {
    @Override
    public ThreadContextSnapshot currentContext(Map<String, String> props) {
        Map<String, String> propagate = MDC.getCopyOfContextMap();
        return () -> {
            Map<String, String> old = MDC.getCopyOfContextMap();
            MDC.setContextMap(propagate);
            return () -> {
                MDC.setContextMap(old);
            };
        };
    }

    @Override
    public ThreadContextSnapshot clearedContext(Map<String, String> props) {
        return () -> {
            Map<String, String> old = MDC.getCopyOfContextMap();
            MDC.clear();
            return () -> {
                MDC.setContextMap(old);
            };
        };
    }

    @Override
    public String getThreadContextType() {
        return "SLF4J MDC";
    }
}