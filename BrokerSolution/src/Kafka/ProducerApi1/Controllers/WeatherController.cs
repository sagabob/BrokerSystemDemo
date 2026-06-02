using Microsoft.AspNetCore.Mvc;

[ApiController]
[Route("api/[controller]")]
public class WeatherController(IWeatherService weatherService) : ControllerBase
{
    [HttpGet]
    public IActionResult Get()
    {
        return Ok("Weather");
    }

    [HttpPost("coordinates")]
    public async Task<IActionResult> Post(Coordinates coordinates)
    {
        var weather = await weatherService.GetWeatherByCoordinates(coordinates);
        return Ok(weather);
    }

    [HttpPost("coordinates/today-hourly")]
    public async Task<IActionResult> GetTodayHourly(Coordinates coordinates)
    {
        var weather = await weatherService.GetTodayHourlyWeatherByCoordinates(coordinates);
        return Ok(weather);
    }
}