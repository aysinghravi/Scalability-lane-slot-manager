package com.acuver.aekyam.slotpoc.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "slot-poc.service-max-slots=1000")
@AutoConfigureMockMvc
class ApiTest {

    @Autowired
    MockMvc mvc;

    @Test
    void onboardAcceptReject() throws Exception {
        mvc.perform(post("/api/v1/connectors").contentType(MediaType.APPLICATION_JSON).content("""
                        {"connectorId":"S3_connector_1","slots":130,"lanes":[
                          {"laneId":"stage-1:bucket1","slots":1,"latencyMs":3000},
                          {"laneId":"stage-2:bucket2","slots":30}]}
                        """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.slots.capacity").value(130))
                .andExpect(jsonPath("$.lanes[0].slots.capacity").value(1))
                .andExpect(jsonPath("$.lanes[1].latencyMs").value(100));

        mvc.perform(post("/api/v1/requests").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"laneId\":\"stage-1:bucket1\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.requestId").isNotEmpty());

        mvc.perform(post("/api/v1/requests").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"laneId\":\"stage-1:bucket1\"}"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().string("Retry-After", "1"))
                .andExpect(jsonPath("$.rejectedBy").value("LANE"))
                .andExpect(jsonPath("$.slotPool").value("stage-1:bucket1"));

        mvc.perform(get("/api/v1/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.service.capacity").value(1000))
                .andExpect(jsonPath("$.service.inFlight").value(1))
                .andExpect(jsonPath("$.connectors[0].lanes[0].slots.rejected").value(1));
    }

    @Test
    void errors() throws Exception {
        mvc.perform(post("/api/v1/requests").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"laneId\":\"no-such-lane\"}"))
                .andExpect(status().isNotFound());

        mvc.perform(post("/api/v1/connectors").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"connectorId\":\"bad\",\"slots\":5,\"lanes\":[]}"))
                .andExpect(status().isBadRequest());

        String body = "{\"connectorId\":\"dup\",\"slots\":5,\"lanes\":[{\"laneId\":\"dup-lane\",\"slots\":1}]}";
        mvc.perform(post("/api/v1/connectors").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated());
        mvc.perform(post("/api/v1/connectors").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isConflict());
    }
}
