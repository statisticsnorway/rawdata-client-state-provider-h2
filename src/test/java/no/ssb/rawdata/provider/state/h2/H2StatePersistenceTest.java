package no.ssb.rawdata.provider.state.h2;

import io.reactivex.Flowable;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.rawdata.api.state.CompletedPosition;
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
        stateProvider.trackCompletedPositions("ns", List.of("a", "b")).blockingGet();
        stateProvider.setNextPosition("ns", "c").blockingGet();
        System.out.printf("next: %s%n", stateProvider.getNextPosition("ns").blockingGet());

        stateProvider.trackCompletedPositions("ns", List.of("c", "d", "e", "f", "g", "h")).blockingGet();
        stateProvider.setNextPosition("ns", "i").blockingGet();

        System.out.printf("first: %s%n", stateProvider.getFirstPosition("ns").blockingGet());
        System.out.printf("last: %s%n", stateProvider.getLastPosition("ns").blockingGet());
        System.out.printf("next: %s%n", stateProvider.getNextPosition("ns").blockingGet());

        System.out.printf("offset: %s%n", stateProvider.getOffsetPosition("ns", "b", 3).blockingGet());

        stateProvider.readPositions("ns", "b", "e").subscribe(onNext -> System.out.printf("%s%n", onNext.position), onError -> onError.printStackTrace());

        Thread.sleep(250);
    }

    @Test //(enabled = false)
    public void testTx() {
        assertTrue(stateProvider.trackCompletedPositions("ns", List.of("a", "b")).blockingGet());
        stateProvider.setNextPosition("ns", "c").blockingGet();
        assertEquals(stateProvider.getLastPosition("ns").blockingGet(), "b");
        assertEquals(stateProvider.getNextPosition("ns").blockingGet(), "c");
        assertEquals(stateProvider.getOffsetPosition("ns", "a", 1).blockingGet(), "b");

        assertTrue(stateProvider.trackCompletedPositions("ns", List.of("c", "d", "e")).blockingGet());
        stateProvider.setNextPosition("ns", "f").blockingGet();
        assertEquals(stateProvider.getLastPosition("ns").blockingGet(), "e");
        assertEquals(stateProvider.getNextPosition("ns").blockingGet(), "f");
        assertEquals(stateProvider.getOffsetPosition("ns", "c", 3).blockingGet(), "e");

        Flowable<CompletedPosition> flowable = stateProvider.readPositions("ns", "b", "e");
        List<String> expected = new ArrayList<>(List.of("b", "c", "d", "e"));
        flowable.subscribe(onNext -> expected.remove(onNext.position), onError -> onError.printStackTrace());
        assertTrue(expected.isEmpty());
    }

}

