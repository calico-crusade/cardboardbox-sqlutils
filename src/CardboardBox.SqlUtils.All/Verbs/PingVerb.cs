namespace CardboardBox.SqlUtils.All.Verbs
{
	[Verb("ping", true, HelpText = "Checks to see if the CLI is working as intended")]
	public class PingVerbOptions { }

	public class PingVerb : SyncVerb<PingVerbOptions>
	{
		private readonly ILogger _logger;

		public PingVerb(ILogger<PingVerb> logger)
		{
			_logger = logger;
		}

		public override int RunSync(PingVerbOptions options)
		{
			_logger.LogInformation("Pong at {0}", DateTime.Now);
			return 0;
		}
	}
}
