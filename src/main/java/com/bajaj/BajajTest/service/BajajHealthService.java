package com.bajaj.BajajTest.service;

import com.bajaj.BajajTest.dto.SolutionRequest;
import com.bajaj.BajajTest.dto.WebhookRequest;
import com.bajaj.BajajTest.dto.WebhookResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
public class BajajHealthService {

    private static final Logger logger = LoggerFactory.getLogger(BajajHealthService.class);
    
    private static final String WEBHOOK_GENERATION_URL = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";
    private static final String SOLUTION_SUBMISSION_URL = "https://bfhldevapigw.healthrx.co.in/hiring/testWebhook/JAVA";

    private final RestTemplate restTemplate;
    private final WebClient webClient;

    public BajajHealthService(RestTemplate restTemplate, WebClient webClient) {
        this.restTemplate = restTemplate;
        this.webClient = webClient;
    }


    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        logger.info("Application started. Beginning Bajaj Health service flow...");
        
        try {
            WebhookResponse webhookResponse = generateWebhook();
            logger.info("Webhook generated successfully: {}", webhookResponse.getWebhook());
            
            String sqlSolution = solveSqlProblem("0101CS221083");
            logger.info("SQL problem solved. Solution: {}", sqlSolution);
            
            submitSolution(webhookResponse.getAccessToken(), sqlSolution);
            logger.info("Solution submitted successfully!");
            
        } catch (Exception e) {
            logger.error("Error in Bajaj Health service flow: ", e);
        }
    }

    public WebhookResponse generateWebhook() {
        logger.info("Generating webhook...");
        
        WebhookRequest request = new WebhookRequest("Neha Kumari", "0101CS221083", "nknehakumari0812@gmail.com");
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        HttpEntity<WebhookRequest> entity = new HttpEntity<>(request, headers);
        
        ResponseEntity<WebhookResponse> response = restTemplate.postForEntity(
            WEBHOOK_GENERATION_URL, 
            entity, 
            WebhookResponse.class
        );
        
        if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
            return response.getBody();
        } else {
            throw new RuntimeException("Failed to generate webhook. Status: " + response.getStatusCode());
        }
    }


    public String solveSqlProblem(String regNo) {
        logger.info("Solving SQL problem for registration number: {}", regNo);
        
        String lastTwoDigits = regNo.substring(regNo.length() - 2);
        int lastTwoDigitsInt = Integer.parseInt(lastTwoDigits);
        
        if (lastTwoDigitsInt % 2 == 0) {
            logger.info("Last two digits are even ({}). Solving Question 2", lastTwoDigits);
            return solveQuestion2();
        } 
        else {
            logger.info("Last two digits are odd ({}). Solving Question 1", lastTwoDigits);
            return solveQuestion1();
        }
    }


    public String solveQuestion1() {
        return "SELECT e1.EMP_ID, e1.FIRST_NAME, e1.LAST_NAME, d.DEPARTMENT_NAME, " +
                   "COUNT(e2.EMP_ID) as YOUNGER_EMPLOYEES_COUNT " +
                   "FROM EMPLOYEE e1 " +
                   "JOIN DEPARTMENT d ON e1.DEPARTMENT = d.DEPARTMENT_ID " +
                   "LEFT JOIN EMPLOYEE e2 ON e1.DEPARTMENT = e2.DEPARTMENT AND e2.DOB > e1.DOB " +
                   "GROUP BY e1.EMP_ID, e1.FIRST_NAME, e1.LAST_NAME, d.DEPARTMENT_NAME " +
                   "ORDER BY e1.EMP_ID DESC;";
    }


    public String solveQuestion2() {
        return "SELECT p.AMOUNT as SALARY, CONCAT(e.FIRST_NAME, ' ', e.LAST_NAME) as NAME, " +
                   "TIMESTAMPDIFF(YEAR, e.DOB, CURDATE()) as AGE, d.DEPARTMENT_NAME " +
                   "FROM PAYMENTS p " +
                   "JOIN EMPLOYEE e ON p.EMP_ID = e.EMP_ID " +
                   "JOIN DEPARTMENT d ON e.DEPARTMENT = d.DEPARTMENT_ID " +
                   "WHERE DAY(p.PAYMENT_TIME) != 1 " +
                   "ORDER BY p.AMOUNT DESC " +
                   "LIMIT 1;";
    }

    public void submitSolution(String accessToken, String sqlSolution) {
        logger.info("Submitting solution with JWT token...");
        
        SolutionRequest request = new SolutionRequest(sqlSolution);
        
        webClient.post()
            .uri(SOLUTION_SUBMISSION_URL)
            .header("Authorization", accessToken)
            .contentType(MediaType.APPLICATION_JSON)
            .body(Mono.just(request), SolutionRequest.class)
            .retrieve()
            .bodyToMono(String.class)
            .doOnSuccess(response -> logger.info("Solution submitted successfully: {}", response))
            .doOnError(error -> logger.error("Error submitting solution: ", error))
            .subscribe();
    }
}
