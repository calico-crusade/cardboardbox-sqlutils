namespace CardboardBox.SqlUtils
{
	using All.Verbs;
	using All.Verbs.CsvToInsert;

	public static class Extensions
	{
		public static IServiceCollection AddSqlUtils(this IServiceCollection services)
		{
			return services;
		}

		public static ICommandLineBuilder AddSqlUtils(this ICommandLineBuilder builder)
		{
			return builder
				.Add<PingVerb, PingVerbOptions>()
				.Add<CsvToInsertVerb, CsvToInsertVerbOptions>();
		}
	}
}