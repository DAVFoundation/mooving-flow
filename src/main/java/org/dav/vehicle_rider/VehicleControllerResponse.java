package org.dav.vehicle_rider;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;

public class VehicleControllerResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    @SerializedName("unitnumber")
    public String UnitNumber;

    @SerializedName("logic_state")
    public int LogicState;
}