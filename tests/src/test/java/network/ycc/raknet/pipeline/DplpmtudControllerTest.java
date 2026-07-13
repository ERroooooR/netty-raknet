package network.ycc.raknet.pipeline;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DplpmtudControllerTest {
    @Test
    public void searchesAboveHandshakeMtuAndRetriesBeforeNarrowing() {
        final DplpmtudController controller = new DplpmtudController(1400, 1500);
        final int candidate = controller.nextProbe(1);
        Assertions.assertTrue(candidate > 1400);
        controller.onProbeSent(candidate);
        controller.onProbeTimeout(candidate, 2);
        Assertions.assertEquals(candidate, controller.nextProbe(3));
        controller.onProbeTimeout(candidate, 4);
        Assertions.assertEquals(candidate, controller.nextProbe(5));
        controller.onProbeTimeout(candidate, 6);
        Assertions.assertTrue(controller.nextProbe(7) < candidate);
        Assertions.assertEquals(1400, controller.confirmedMtu());
    }

    @Test
    public void validatesPacketTooBigBoundsBeforeChangingPath() {
        final DplpmtudController controller = new DplpmtudController(1400, 1500);
        controller.onPacketTooBig(1450, 1400);
        Assertions.assertEquals(1400, controller.confirmedMtu());
        controller.onPacketTooBig(1280, 1400);
        Assertions.assertEquals(1280, controller.confirmedMtu());
        Assertions.assertEquals(DplpmtudController.State.BASE, controller.state());
    }
}
