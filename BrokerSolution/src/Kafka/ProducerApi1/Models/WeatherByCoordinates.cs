using System.Text.Json.Serialization;

public class CurrentWeather
{
    [JsonPropertyName("time")]
    public DateTime Time { get; set; } 

    [JsonPropertyName("temperature_2m")]
    public double Temperature2m { get; set; }

    [JsonPropertyName("wind_speed_10m")]
    public double WindSpeed10m { get; set; }

    [JsonPropertyName("relative_humidity_2m")]
    public int RelativeHumidity2m { get; set; }
}

public class OpenMeteoResponse
{
    [JsonPropertyName("current")]
    public CurrentWeather Current { get; set; } = new();
}
public class WeatherByCoordinates
{
    public CurrentWeather CurrentWeather { get; set; } = new();
    public Coordinates InputCoordinates { get; set; } = new();
}

public class HourlyWeather
{
    public DateTime Time { get; set; }
    public double Temperature2m { get; set; }
    public double WindSpeed10m { get; set; }
    public int RelativeHumidity2m { get; set; }
}

public class TodayHourlyWeatherByCoordinates
{
    public DateOnly Date { get; set; }
    public string Timezone { get; set; } = "";
    public List<HourlyWeather> Hours { get; set; } = [];
    public Coordinates InputCoordinates { get; set; } = new();
}

public class OpenMeteoHourlyData
{
    [JsonPropertyName("time")]
    public List<string> Time { get; set; } = [];

    [JsonPropertyName("temperature_2m")]
    public List<double> Temperature2m { get; set; } = [];

    [JsonPropertyName("wind_speed_10m")]
    public List<double> WindSpeed10m { get; set; } = [];

    [JsonPropertyName("relative_humidity_2m")]
    public List<int> RelativeHumidity2m { get; set; } = [];
}

public class OpenMeteoHourlyResponse
{
    [JsonPropertyName("timezone")]
    public string Timezone { get; set; } = "";

    [JsonPropertyName("utc_offset_seconds")]
    public int UtcOffsetSeconds { get; set; }

    [JsonPropertyName("hourly")]
    public OpenMeteoHourlyData Hourly { get; set; } = new();
}