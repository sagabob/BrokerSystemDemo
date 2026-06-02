using Microsoft.AspNetCore.Mvc;

namespace BrokerSolution.Kafka.ProducerApi1.Controllers;

[ApiController]
[Route("api/[controller]")]
public class HealthController : ControllerBase
{
    [HttpGet]
    public IActionResult Get()
    {
        return Ok("OK");
    }
}