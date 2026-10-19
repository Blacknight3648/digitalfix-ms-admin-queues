package df.digitalfix_ms_admin_queues.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ListenerContainerDTO {

    private String listenerId;
    private boolean running;
    private boolean paused;
    private boolean autoStartup;
    private String queueNames;
}
