public class WeatherService(HttpClient httpClient) : IWeatherService
{
    public async Task<WeatherByCoordinates> GetWeatherByCoordinates(Coordinates coordinates)
    {
        ValidateCoordinates(coordinates);

        var response = await httpClient.GetFromJsonAsync<OpenMeteoResponse>(
            BuildForecastUrl(
                coordinates,
                "current=temperature_2m,relative_humidity_2m,wind_speed_10m"));

        if (response is null)
        {
            throw new HttpRequestException("Open-Meteo API returned an empty response.");
        }

        return new WeatherByCoordinates
        {
            CurrentWeather = response.Current,
            InputCoordinates = coordinates
        };
    }

    public async Task<TodayHourlyWeatherByCoordinates> GetTodayHourlyWeatherByCoordinates(Coordinates coordinates)
    {
        ValidateCoordinates(coordinates);

        var utcToday = DateOnly.FromDateTime(DateTime.UtcNow);
        var response = await httpClient.GetFromJsonAsync<OpenMeteoHourlyResponse>(
            BuildForecastUrl(
                coordinates,
                "hourly=temperature_2m,relative_humidity_2m,wind_speed_10m",
                $"&start_date={utcToday.AddDays(-1):yyyy-MM-dd}&end_date={utcToday.AddDays(1):yyyy-MM-dd}"));

        if (response is null)
        {
            throw new HttpRequestException("Open-Meteo API returned an empty response.");
        }

        var localToday = DateOnly.FromDateTime(
            DateTime.UtcNow.AddSeconds(response.UtcOffsetSeconds));

        var todayHours = MapHourly(response.Hourly)
            .Where(h => DateOnly.FromDateTime(h.Time) == localToday)
            .OrderBy(h => h.Time)
            .ToList();

        if (todayHours.Count == 0)
        {
            throw new HttpRequestException("Open-Meteo API returned no hourly data for today.");
        }

        return new TodayHourlyWeatherByCoordinates
        {
            Date = localToday,
            Timezone = response.Timezone,
            Hours = todayHours,
            InputCoordinates = coordinates
        };
    }

    private static void ValidateCoordinates(Coordinates coordinates)
    {
        if (coordinates.Latitude is < -90 or > 90)
        {
            throw new ArgumentOutOfRangeException(nameof(coordinates.Latitude), "Latitude must be between -90 and 90.");
        }

        if (coordinates.Longitude is < -180 or > 180)
        {
            throw new ArgumentOutOfRangeException(nameof(coordinates.Longitude), "Longitude must be between -180 and 180.");
        }
    }

    private static string BuildForecastUrl(Coordinates coordinates, string query, string extraQuery = "") =>
        $"https://api.open-meteo.com/v1/forecast?latitude={coordinates.Latitude}&longitude={coordinates.Longitude}&{query}&timezone=auto{extraQuery}";

    private static List<HourlyWeather> MapHourly(OpenMeteoHourlyData hourly)
    {
        var count = hourly.Time.Count;
        var hours = new List<HourlyWeather>(count);

        for (var i = 0; i < count; i++)
        {
            hours.Add(new HourlyWeather
            {
                Time = DateTime.Parse(hourly.Time[i]),
                Temperature2m = hourly.Temperature2m[i],
                WindSpeed10m = hourly.WindSpeed10m[i],
                RelativeHumidity2m = hourly.RelativeHumidity2m[i]
            });
        }

        return hours;
    }
}
