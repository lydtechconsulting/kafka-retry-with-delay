package demo.properties;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Configuration
@ConfigurationProperties("demo")
@Getter
@Setter
@Validated
public class DemoProperties {

    private @NotNull @Valid Topics topics;

    @Getter
    @Setter
    public static class Topics {
        @NotNull
        private String itemUpdateTopic;
        @NotNull
        private String retryTopic;
    }
}
