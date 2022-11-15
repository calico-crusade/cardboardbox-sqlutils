return await new ServiceCollection()
	.AddSerilog()
	.AddSqlUtils()
	.Cli(c => c.AddSqlUtils());