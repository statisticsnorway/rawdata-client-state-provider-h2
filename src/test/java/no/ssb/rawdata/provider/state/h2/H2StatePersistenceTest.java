package no.ssb.rawdata.provider.state.h2;

import io.reactivex.Flowable;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.rawdata.api.state.CompletedPosition;
import no.ssb.rawdata.api.state.LinkedSet;
import no.ssb.rawdata.api.state.StatePersistenceInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class H2StatePersistenceTest {

    private static final Logger LOG = LoggerFactory.getLogger(H2StatePersistenceTest.class);

    private DynamicConfiguration configuration;
    private H2StatePersistence stateProvider;

    static DynamicConfiguration configuration() {
        Path currentPath = Paths.get("").toAbsolutePath().resolve("target");
        return new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource("application-defaults.properties")
                .propertiesResource("application-test.properties")
                .values("storage.provider", "h2")
                .build();
    }

    static H2StatePersistence storageProvider(DynamicConfiguration configuration) {
        return ProviderConfigurator.configure(configuration, "h2", StatePersistenceInitializer.class);
    }

    @BeforeClass
    public void setUp() {
        configuration = configuration();
        stateProvider = storageProvider(configuration);
        assertNotNull(stateProvider);
    }

    @Test(enabled = false)
    public void testName() throws InterruptedException {
        stateProvider.expectedPagePositions("ns", LinkedSet.of("a", "b", "c", "d", "e")).blockingGet();
        stateProvider.expectedPagePositions("ns", LinkedSet.of("f", "g", "h", "i", "j")).blockingGet();
        stateProvider.expectedPagePositions("ns", LinkedSet.of("k", "l", "m", "n", "o")).blockingGet();

        stateProvider.trackCompletedPositions("ns", LinkedSet.of("a", "b")).blockingGet();
//        stateProvider.trackCompletedPositions("ns", LinkedSet.of("b", "c")).blockingGet();
//        stateProvider.trackCompletedPositions("ns", LinkedSet.of("a", "b", "c", "d", "e")).blockingGet();
        stateProvider.trackCompletedPositions("ns", LinkedSet.of("g", "h")).blockingGet();
        stateProvider.trackCompletedPositions("ns", LinkedSet.of("k", "l")).blockingGet();

        System.out.printf("%s%n", stateProvider.getFirstPosition("ns").blockingGet());
        System.out.printf("%s%n", stateProvider.getNextPosition("ns").blockingGet());
        stateProvider.readPositions("ns", "a", "c").subscribe(onNext -> System.out.printf("%s%n", onNext.position), onError -> onError.printStackTrace());

        Thread.sleep(250);
    }

    @Test //(enabled = false)
    public void testTx() {
        assertTrue(stateProvider.expectedPagePositions("ns", LinkedSet.of("a", "b", "c", "d", "e")).blockingGet());
        assertTrue(stateProvider.expectedPagePositions("ns", LinkedSet.of("f", "g", "h", "i", "j")).blockingGet());

        assertTrue(stateProvider.trackCompletedPositions("ns", LinkedSet.of("a", "b")).blockingGet());
        assertEquals(stateProvider.getNextPosition("ns").blockingGet(), "c");
        assertEquals(stateProvider.getLastPosition("ns").blockingGet(), "b");
//        assertEquals(stateProvider.getOffsetPosition("ns", "a", 1).blockingGet(), "b");

        assertTrue(stateProvider.trackCompletedPositions("ns", LinkedSet.of("g", "h", "i", "j")).blockingGet());
        assertEquals(stateProvider.getNextPosition("ns").blockingGet(), "c");
        assertEquals(stateProvider.getLastPosition("ns").blockingGet(), "b");

        assertTrue(stateProvider.expectedPagePositions("ns", LinkedSet.of("c", "d", "e", "f", "g")).blockingGet());
        assertTrue(stateProvider.trackCompletedPositions("ns", LinkedSet.of("c", "d", "e")).blockingGet());
        assertEquals(stateProvider.getNextPosition("ns").blockingGet(), "f");
        assertEquals(stateProvider.getLastPosition("ns").blockingGet(), "e");

        assertTrue(stateProvider.expectedPagePositions("ns", LinkedSet.of("k", "l", "m", "n", "o")).blockingGet());
        assertTrue(stateProvider.trackCompletedPositions("ns", LinkedSet.of("l", "m", "n")).blockingGet());

        assertEquals(stateProvider.getNextPosition("ns").blockingGet(), "f");
        assertEquals(stateProvider.getLastPosition("ns").blockingGet(), "e");
        assertTrue(stateProvider.trackCompletedPositions("ns", LinkedSet.of("f")).blockingGet());
        assertEquals(stateProvider.getNextPosition("ns").blockingGet(), "k");
        assertEquals(stateProvider.getLastPosition("ns").blockingGet(), "j");

        assertTrue(stateProvider.trackCompletedPositions("ns", LinkedSet.of("k", "o")).blockingGet());
        assertNull(stateProvider.getNextPosition("ns").blockingGet());
        assertNotNull(stateProvider.getLastPosition("ns").blockingGet());

        assertTrue(stateProvider.expectedPagePositions("ns", LinkedSet.of("p", "q", "r", "s", "t")).blockingGet());
        assertTrue(stateProvider.trackCompletedPositions("ns", LinkedSet.of("q")).blockingGet());
        assertEquals(stateProvider.getNextPosition("ns").blockingGet(), "p");
        assertEquals(stateProvider.getLastPosition("ns").blockingGet(), "o");


        Flowable<CompletedPosition> flowable = stateProvider.readPositions("ns", "b", "m");
        List<String> expected = new ArrayList<>(List.of("b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"));
        flowable.subscribe(onNext -> expected.remove(onNext.position), onError -> onError.printStackTrace());
        assertTrue(expected.isEmpty());

        assertTrue(stateProvider.trackCompletedPositions("ns", LinkedSet.of("p", "r", "s", "t")).blockingGet());
        stateProvider.readPositions("ns", "a", "t").subscribe(
                onNext -> System.out.printf("%s%n", onNext),
                onError -> onError.printStackTrace(),
                () -> assertEquals(stateProvider.getLastPosition("ns").blockingGet(), "t")
        );

//        assertEquals(stateProvider.getOffsetPosition("ns", "a", 2).blockingGet(), "b");
    }

}

