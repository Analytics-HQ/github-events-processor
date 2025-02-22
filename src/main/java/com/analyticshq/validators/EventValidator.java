package com.analyticshq.validators;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
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
