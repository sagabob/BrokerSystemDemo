public interface IWeatherService
{
    Task<WeatherByCoordinates> GetWeatherByCoordinates(Coordinates coordinates);
    Task<TodayHourlyWeatherByCoordinates> GetTodayHourlyWeatherByCoordinates(Coordinates coordinates);
}