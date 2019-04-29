import no.ssb.rawdata.api.state.StatePersistenceInitializer;
import no.ssb.rawdata.provider.state.h2.H2StatePersistenceInitializer;

module no.ssb.rawdata.state.provider.h2db {
    requires java.base;
    requires java.logging;
    requires org.slf4j;
    requires io.reactivex.rxjava2;
    requires org.reactivestreams;
    requires java.sql;
    requires com.zaxxer.hikari;
    requires com.h2database;
    requires no.ssb.config;
    requires no.ssb.rawdata.api;
    requires no.ssb.service.provider.api;

    provides StatePersistenceInitializer with H2StatePersistenceInitializer;

    opens h2;

    exports no.ssb.rawdata.provider.state.h2;

}
