using System.Globalization;
using CsvHelper;
using LibArchive.Net;
using Spectre.Console;

namespace Unpack;

public static class Unpack
{
    public static void Main(string[] args)
    {
        if (args.Length != 2)
        {
            Console.Error.WriteLine("Usage: Unpack <csv file> <zip directory>");
            return;
        }
        using var reader = new StreamReader(args[0]);
        using var csv = new CsvReader(reader,CultureInfo.InvariantCulture);
        var records = csv.GetRecords<dynamic>();
        var workList = records.SelectMany(r => (IDictionary<string, object>)r, (r, field) => field.Value.ToString())
            .Where(s => s?.Contains('!') == true).Select(path => path?.Split('!', 2)).GroupBy(bits => bits?[0])
            .ToDictionary(zip => zip.Key ?? "",
                zip => zip.Select(a => a?[1] ?? "").Where(a => !string.IsNullOrWhiteSpace(a)).ToHashSet());

        AnsiConsole.Progress()
            .Columns(new ProgressColumn[]
            {
                new TaskDescriptionColumn(),    // Task description
                new ProgressBarColumn(),        // Progress bar
                new PercentageColumn(),         // Percentage
                new RemainingTimeColumn(),      // Remaining time
                new SpinnerColumn(),            // Spinner
            })
            .Start(ctx =>
            {
                var task=ctx.AddTask("[green]Unpacking...[/]");
                task.MaxValue=workList.Count;
                Parallel.ForEach(workList, (job, _) =>
                {
                    ProcessArchive(args[1], job.Key, job.Value);
                    task.Increment(1);
                });
            }); 
    }

    private static void ProcessArchive(string archiveDir, string zipName, ICollection<string> targets)
    {
        try
        {
            using var zip = new LibArchiveReader(Path.Combine(archiveDir, zipName));
            foreach (var entry in zip.Entries())
            {
                if (!targets.Contains(entry.Name)) continue;
                using var outFile = File.OpenWrite(entry.Name);
                using var src = entry.Stream;
                src.CopyTo(outFile, 1 << 20);
                targets.Remove(entry.Name);
            }

            if (targets.Count == 0) return;
            Console.Error.WriteLine($"WARN:{zipName}:missing {string.Join(',', targets)}");
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"ERROR:{zipName}:{e.Message}");
        }
    }
}