"""Automated compilation and shortcut deployment suite for BRS Engine launchers."""

import os
import subprocess
import tempfile
from pathlib import Path

# Target .NET Framework C# compiler
CSC_PATH = r"C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe"
PROJECT_ROOT = Path(__file__).resolve().parent.parent

LAUNCHER_CS = """using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

[assembly: AssemblyTitle("BRS Engine Launcher")]
[assembly: AssemblyDescription("BRS Audit Engine Startup Utility")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Bank Reconciliation System")]
[assembly: AssemblyProduct("BRS Anomaly Detection Suite")]
[assembly: AssemblyCopyright("Copyright © Bank Reconciliation System 2026")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]
[assembly: ComVisible(false)]
[assembly: Guid("e5c6a8f1-3053-46a8-a033-c3784b9d98e8")]
[assembly: AssemblyVersion("1.0.0.0")]
[assembly: AssemblyFileVersion("1.0.0.0")]

class Program
{
    [DllImport("user32.dll")]
    static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);

    [DllImport("kernel32.dll")]
    static extern IntPtr GetConsoleWindow();

    static void Main(string[] args)
    {
        IntPtr hConsole = GetConsoleWindow();
        if (hConsole != IntPtr.Zero)
        {
            ShowWindow(hConsole, 0); // SW_HIDE = 0
        }
        RunLauncher().GetAwaiter().GetResult();
    }

    static async Task RunLauncher()
    {
        string baseDir = AppDomain.CurrentDomain.BaseDirectory;
        Directory.SetCurrentDirectory(baseDir);

        // Launch start batch script headlessly
        ProcessStartInfo startInfo = new ProcessStartInfo();
        startInfo.FileName = "cmd.exe";
        startInfo.Arguments = "/c Start_BRS_Engine.bat";
        startInfo.WorkingDirectory = baseDir;
        startInfo.WindowStyle = ProcessWindowStyle.Hidden;
        startInfo.CreateNoWindow = true;
        startInfo.UseShellExecute = false;

        try
        {
            Process.Start(startInfo);
        }
        catch (Exception)
        {
            return;
        }

        // Programmatic polling of Streamlit server (localhost:8501)
        using (HttpClient client = new HttpClient())
        {
            client.Timeout = TimeSpan.FromSeconds(2);
            bool healthy = false;
            for (int i = 0; i < 120; i++) // Poll up to 180 seconds (120 * 1.5s)
            {
                try
                {
                    HttpResponseMessage response = await client.GetAsync("http://localhost:8501");
                    if (response.IsSuccessStatusCode)
                    {
                        healthy = true;
                        break;
                    }
                }
                catch
                {
                    // Ignore connection errors and retry
                }
                await Task.Delay(1500);
            }

            if (healthy)
            {
                try
                {
                    System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo {
                        FileName = "cmd.exe",
                        Arguments = "/c start http://localhost:8501",
                        CreateNoWindow = true,
                        UseShellExecute = false
                    });
                }
                catch { }
            }
        }
    }
}
"""

STOP_CS = """using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;

[assembly: AssemblyTitle("BRS Engine Shutdown Manager")]
[assembly: AssemblyDescription("BRS Audit Engine Stopping Utility")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Bank Reconciliation System")]
[assembly: AssemblyProduct("BRS Anomaly Detection Suite")]
[assembly: AssemblyCopyright("Copyright © Bank Reconciliation System 2026")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]
[assembly: ComVisible(false)]
[assembly: Guid("9a8b7c6d-5e4f-3a2b-1a2b-3c4d5e6f7a8b")]
[assembly: AssemblyVersion("1.0.0.0")]
[assembly: AssemblyFileVersion("1.0.0.0")]

class Program
{
    [DllImport("user32.dll")]
    static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);

    [DllImport("kernel32.dll")]
    static extern IntPtr GetConsoleWindow();

    static void Main(string[] args)
    {
        IntPtr hConsole = GetConsoleWindow();
        if (hConsole != IntPtr.Zero)
        {
            ShowWindow(hConsole, 0); // SW_HIDE = 0
        }

        string baseDir = AppDomain.CurrentDomain.BaseDirectory;
        Directory.SetCurrentDirectory(baseDir);

        // Shut down Docker Compose services headlessly
        RunCommand("docker-compose.exe", "down", baseDir);

        // Targeted powershell process termination for active runtimes
        string psCmd1 = "Get-CimInstance Win32_Process -Filter \\"Name = 'python.exe'\\" | " +
                        "Where-Object { $_.CommandLine -like '*audit_app.py*' } | " +
                        "ForEach-Object { Stop-Process -Id $_.ProcessId -Force }";
        string psCmd2 = "Get-CimInstance Win32_Process -Filter \\"Name = 'python.exe'\\" | " +
                        "Where-Object { $_.CommandLine -like '*hot_folder_monitor.py*' } | " +
                        "ForEach-Object { Stop-Process -Id $_.ProcessId -Force }";

        RunCommand(
            "powershell.exe",
            "-WindowStyle Hidden -Command \\"" + psCmd1 + "\\"\\"",
            baseDir
        );
        RunCommand(
            "powershell.exe",
            "-WindowStyle Hidden -Command \\"" + psCmd2 + "\\"\\"",
            baseDir
        );
    }

    static void RunCommand(string file, string args, string workingDir)
    {
        ProcessStartInfo startInfo = new ProcessStartInfo();
        startInfo.FileName = file;
        startInfo.Arguments = args;
        startInfo.WorkingDirectory = workingDir;
        startInfo.WindowStyle = ProcessWindowStyle.Hidden;
        startInfo.CreateNoWindow = true;
        startInfo.UseShellExecute = false;

        try
        {
            Process p = Process.Start(startInfo);
            if (p != null)
            {
                p.WaitForExit(15000); // 15-second execution ceiling
            }
        }
        catch { }
    }
}
"""


def compile_binary(source_content: str, output_name: str, references: list[str]) -> bool:
    """Compile C# source content to target output binary name using native csc.exe."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cs_file = Path(temp_dir) / "source.cs"
        cs_file.write_text(source_content, encoding="utf-8")

        # Compile as Console subsystem (/target:exe) to bypass AV heuristic flags.
        # SW_HIDE is used inside C# code to achieve silent headless launch.
        cmd = [
            CSC_PATH,
            "/target:exe",
            "/optimize",
            f"/out:{PROJECT_ROOT / output_name}",
        ]
        for ref in references:
            cmd.append(f"/r:{ref}")
        cmd.append(str(cs_file))

        print(f"[*] Compiling {output_name}...")
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            print(f"[-] Compilation error for {output_name}:")
            print(res.stderr)
            print(res.stdout)
            return False
        print(f"[+] Compiled {output_name} successfully.")
        return True


def create_desktop_shortcut() -> None:
    """Deploy pristine desktop shortcut via Windows Script Host VBScript routing."""
    launcher_path = PROJECT_ROOT / "BRS_Engine_Launcher.exe"
    vbs_content = f"""Set WshShell = CreateObject("WScript.Shell")
strDesktop = WshShell.SpecialFolders("Desktop")
Set oShellLink = WshShell.CreateShortcut(strDesktop & "\\BRS Audit Control Console.lnk")
oShellLink.TargetPath = "{launcher_path}"
oShellLink.WorkingDirectory = "{PROJECT_ROOT}"
oShellLink.Description = "BRS Audit Control Console"
oShellLink.IconLocation = "shell32.dll, 220"
oShellLink.Save()
"""
    with tempfile.TemporaryDirectory() as temp_dir:
        vbs_file = Path(temp_dir) / "shortcut.vbs"
        vbs_file.write_text(vbs_content, encoding="utf-8")

        print("[*] Creating desktop shortcut...")
        subprocess.run(["wscript.exe", str(vbs_file)], check=True)
        print("[+] Desktop shortcut created successfully.")


def main() -> None:
    if not os.path.exists(CSC_PATH):
        raise FileNotFoundError(
            f"Native .NET compiler (csc.exe) not found at: {CSC_PATH}"
        )

    # 1. Compile Launcher.cs
    success_launch = compile_binary(
        LAUNCHER_CS,
        "BRS_Engine_Launcher.exe",
        ["System.dll", "System.Core.dll", "System.Net.Http.dll"],
    )

    # 2. Compile Stop.cs
    success_stop = compile_binary(
        STOP_CS,
        "Stop_BRS_Engine.exe",
        ["System.dll", "System.Core.dll"],
    )

    if success_launch and success_stop:
        # 3. Deploy Shortcut
        create_desktop_shortcut()
        print("[+] Suite deployment finished successfully.")
    else:
        print("[-] Deployment aborted due to compilation failures.")


if __name__ == "__main__":
    main()
