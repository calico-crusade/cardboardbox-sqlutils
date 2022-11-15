namespace CardboardBox.SqlUtils.Core
{
	public interface ICsvLineReader : IAsyncDisposable
	{
		Task<string[]> ReadHeader();

		IAsyncEnumerable<string[]> ReadLines();
	}

	public class CsvLineReader : ICsvLineReader
	{
		private readonly Stream _input;
		private readonly StreamReader _reader;
		private readonly bool _leaveOpen;
		private readonly bool _hasHeader;

		public CsvLineReader(Stream input, bool hasHeader = true, bool leaveOpen = true)
		{
			_input = input;
			_reader = new StreamReader(input, leaveOpen: true);
			_leaveOpen = leaveOpen;
			_hasHeader = hasHeader;
		}

		public async Task<string[]> ReadHeader()
		{
			if (!_hasHeader) return Array.Empty<string>();

			var header = await _reader.ReadLineAsync();
			if (header == null) return Array.Empty<string>();

			return ParseLine(header);
		}

		public async IAsyncEnumerable<string[]> ReadLines()
		{
			while(!_reader.EndOfStream)
			{
				var line = await _reader.ReadLineAsync();
				if (line == null) yield break;

				yield return ParseLine(line);
			}
		}

		public string[] ParseLine(string line)
		{
			return line.Split(',')
				.Select(t => t.Trim('"').Trim())
				.ToArray();
		}

		public async ValueTask DisposeAsync()
		{
			_reader.Dispose();
			if (!_leaveOpen) await _input.DisposeAsync();
			GC.SuppressFinalize(this);
		}

		public static ICsvLineReader Open(string path, bool hasHeader = true, bool leaveOpen = false)
		{
			return Open(File.OpenRead(path), hasHeader, leaveOpen);
		}

		public static ICsvLineReader Open(Stream input, bool hasHeader = true, bool leaveOpen = true)
		{
			return new CsvLineReader(input, hasHeader, leaveOpen);
		}
	}
}
