package org.dav.vehicle_rider;

import static org.junit.Assert.assertEquals;
import java.util.UUID;
import org.dav.Json;
import org.dav.vehicle_rider.LockVehicleFlow.LockVehicleMessage;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockVehicleFlowTest {
    private static final Logger LOG = LoggerFactory.getLogger(LockVehicleFlowTest.class);

    // @Test
    public void test1() {
        String msg =
                "{\"stateChanged\":false,\"vehicleId\":\"01167293-1e6f-48a6-bfcb-ec0c0e58fd08\"}";
        LockVehicleMessage p = Json.parse(msg, LockVehicleMessage.class, false);
        assertEquals(UUID.fromString("01167293-1e6f-48a6-bfcb-ec0c0e58fd08"),p.getVehicleId());
    }
}
