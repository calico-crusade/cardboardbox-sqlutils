namespace CardboardBox.SqlUtils.All.Verbs.CsvToInsert
{
	[Verb("csv-to-inserts", false, new[] { "cti" }, HelpText = "Converts the given CSV to an insert statement")]
	public class CsvToInsertVerbOptions
	{
		[Option('p', "path", HelpText = "The path to the CSV", Required = true)]
		public string Path { get; set; } = string.Empty;

		[Option('o', "output", HelpText = "Where to save the outputted SQL query, defaults to 'output.sql'", Required = false, Default = "output.sql")]
		public string Output { get; set; } = string.Empty;

		[Option('s', "splits", HelpText = "How to order the columns in the CSV (comma separated array of the column indexes in the order they should appear), leaving blank utilizes the natural indexes", Required = false)]
		public string? Splits { get; set; }

		[Option('r', "record-split", HelpText = "Splits the records into separate chunks of inserts, leaving blank puts them all in the same query. This is useful for SQL server as it limits inserts to 1000 records", Required = false)]
		public int? RecordSplit { get; set; }

		[Option('h', "has-header", HelpText = "Whether or not to ignore the first record in the file because it's a header record. Defaults to true", Required = false, Default = true)]
		public bool HasHeader { get; set; } = true;

		[Option('u', "use-headers-as-cols", HelpText = "Whether or not to use the headers as the column names in the exported queries. Defaults to true. This requires the 'has-header' to be true.", Required = false, Default = true)]
		public bool UseHeaderAsColumnName { get; set; } = true;

		[Option('t', "table-name", HelpText = "The name of the table to use for the insert query.", Required = true)]
		public string TableName { get; set; } = string.Empty;

		[Option('c', "columns", HelpText = "The names of the columns (comma separated). Required it `use-headers-as-cols` and `has-header` are false.", Required = false)]
		public string? Columns { get; set; }

		[Option('e', "escape-char", HelpText = "The character to use to escape single quotes in the query. Defaults to a single quote.", Required = false, Default = "'")]
		public string EscapeCharacter { get; set; } = "'";
	}

	public class CsvToInsertVerb : IVerb<CsvToInsertVerbOptions>
	{
		private readonly ILogger _logger;

		public CsvToInsertVerb(
			ILogger<CsvToInsertVerb> logger)
		{
			_logger = logger;
		}

		public async Task<int> Run(CsvToInsertVerbOptions options)
		{
			if (!File.Exists(options.Path))
			{
				_logger.LogError($"\"{options.Path}\" does not exist.");
				return 1;
			}

			using var io = File.Create(options.Output);
			using var output = new StreamWriter(io);
			var reader = CsvLineReader.Open(options.Path, options.HasHeader);

			var cols = await DetermineColumns(options, reader);
			var order = DetermineOrder(options, cols);
			cols = Sort(order, cols);
			var queryPart = GenerateQuery(cols, options);

			await output.WriteLineAsync(queryPart);

			int i = -1;
			await foreach(var record in reader.ReadLines())
			{
				i++;

				if (options.RecordSplit != null && i > options.RecordSplit - 1)
				{
					await output.WriteLineAsync(";");
					await output.WriteLineAsync();
					await output.WriteLineAsync(queryPart);
					i = 0;
				}
				else if (i != 0) await output.WriteLineAsync(',');

				var escaped = Sort(order, Escape(record, options));
				await output.WriteAsync($"({string.Join(", ", escaped)})");
			}

			await output.WriteAsync(";");
			await output.FlushAsync();

			return 0;
		}

		public async Task<string[]> DetermineColumns(CsvToInsertVerbOptions opts, ICsvLineReader reader)
		{
			if (opts.UseHeaderAsColumnName && !opts.HasHeader)
				throw new ArgumentException("In order to use `use-headers-as-cols` the `has-header` options has to be set to true", "has-header");

			if (opts.HasHeader && opts.UseHeaderAsColumnName)
			{
				var header = await reader.ReadHeader();
				if (header.Length == 0)
					throw new Exception("File has no headers, but `has-header` is set to true");

				return header;
			}

			if (string.IsNullOrEmpty(opts.Columns))
				throw new ArgumentException("`use-headers-as-cols` is false but `columns` is empty", "columns");

			var parts = opts.Columns.Split(',').Select(t => t.Trim()).ToArray();
			if (parts.Length == 0)
				throw new ArgumentException("`columns` is invalid. Cannot find any columns to use.", "columns");

			return parts;
		}

		public string GenerateQuery(string[] columns, CsvToInsertVerbOptions opts)
		{
			return $"INSERT INTO {opts.TableName} ({string.Join(", ", columns)}) VALUES ";
		}

		public string[] Escape(string[] record, CsvToInsertVerbOptions opts)
		{
			return record.Select(t =>
			{
				var escape = opts.EscapeCharacter;
				return $"'{t.Replace("'", opts.EscapeCharacter + "'")}'";
			}).ToArray();
		}

		public int[] DetermineOrder(CsvToInsertVerbOptions opts, string[] columns)
		{
			if (string.IsNullOrEmpty(opts.Splits)) return columns.Select((t, i) => i).ToArray();

			return opts.Splits.Split(',')
				.Select(t =>
				{
					if (!int.TryParse(t.Trim(), out int index) || index < 0 || index + 1 > columns.Length)
						throw new ArgumentException($"`{t}` is not a valid column index");

					return index;
				}).ToArray();
		}

		public string[] Sort(int[] order, string[] record)
		{
			return order.Select(t => record[t]).ToArray();
		}
	}
}
