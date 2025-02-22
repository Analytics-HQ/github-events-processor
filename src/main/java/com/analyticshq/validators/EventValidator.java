package com.analyticshq.validators;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.Set;

import com.analyticshq.models.GithubEvent;

public class EventValidator {
    private static final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    private static final Validator validator = factory.getValidator();

    public static boolean isValid(GithubEvent event) {
        Set<ConstraintViolation<GithubEvent>> violations = validator.validate(event);
        return violations.isEmpty();
    }
}
