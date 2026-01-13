$BasePath = "D:\LearningMaterials\Slow SQL\BIRD-CRITIC-1\evaluation\postgre_table_dumps"
$PGUser = "root"
$PGHost = "localhost"
$PGPort = "5432"
$PGPassword = "123456" 
$env:PGPASSWORD = $PGPassword
set PGPASSWORD "123456"
Write-Host "[INFO] Starting batch import of SQL templates..." -ForegroundColor Cyan

$templateDirs = Get-ChildItem -Path $BasePath -Directory | Where-Object { $_.Name -like "*_template" }

if ($templateDirs.Count -eq 0) {
    Write-Warning "No *_template directories found."
    exit 1
}

foreach ($dir in $templateDirs) {
    $dbName = $dir.Name -replace '_template$', ''
    Write-Host "`n[PROCESSING] Template: $($dir.Name) => Database: $dbName" -ForegroundColor Yellow

    # Check if database exists
    $output = psql -U $PGUser -h $PGHost -p $PGPort -t -A -c "SELECT 1 FROM pg_database WHERE datname = '$dbName';" postgres 2>$null
    $dbExists = $output -eq "1"

    if (-not $dbExists) {
        Write-Host "  [CREATE] Creating database: $dbName"
        createdb -U $PGUser -h $PGHost -p $PGPort $dbName
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to create database: $dbName"
            continue
        }
    } else {
        Write-Host "  [SKIP] Database already exists: $dbName"
    }

    $sqlFiles = Get-ChildItem -Path $dir.FullName -Filter "*.sql"
    if ($sqlFiles.Count -eq 0) {
        Write-Host "  [WARN] No .sql files found in directory."
        continue
    }

    # Analyze dependencies and sort files
    $fileGraph = @{}
    $allTables = @{}
    
    # First pass: collect all tables created by each file
    foreach ($file in $sqlFiles) {
        $content = Get-Content $file.FullName -Raw -Encoding UTF8
        $tables = [regex]::Matches($content, '(?i)CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+(?:public\.)?(["\w]+)', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        $tableNames = $tables | ForEach-Object { $_.Groups[1].Value.Trim('"') }
        
        $fileGraph[$file.Name] = @{
            File = $file
            Creates = $tableNames
            DependsOn = @()
        }
        
        foreach ($table in $tableNames) {
            $allTables[$table] = $file.Name
        }
    }
    
    # Second pass: find dependencies (foreign keys and references)
    foreach ($file in $sqlFiles) {
        $content = Get-Content $file.FullName -Raw -Encoding UTF8
        
        # Find FOREIGN KEY references
        $fkMatches = [regex]::Matches($content, '(?i)REFERENCES\s+(?:public\.)?(["\w]+)', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        
        foreach ($match in $fkMatches) {
            $referencedTable = $match.Groups[1].Value.Trim('"')
            if ($allTables.ContainsKey($referencedTable)) {
                $dependentFile = $allTables[$referencedTable]
                if ($dependentFile -ne $file.Name -and $fileGraph[$file.Name].DependsOn -notcontains $dependentFile) {
                    $fileGraph[$file.Name].DependsOn += $dependentFile
                }
            }
        }
    }
    
    # Topological sort using Kahn's algorithm
    $sortedFiles = @()
    $inDegree = @{}
    
    foreach ($fileName in $fileGraph.Keys) {
        $inDegree[$fileName] = 0
    }
    
    foreach ($fileName in $fileGraph.Keys) {
        foreach ($dep in $fileGraph[$fileName].DependsOn) {
            $inDegree[$fileName]++
        }
    }
    
    $queue = New-Object System.Collections.Queue
    foreach ($fileName in $inDegree.Keys) {
        if ($inDegree[$fileName] -eq 0) {
            $queue.Enqueue($fileName)
        }
    }
    
    while ($queue.Count -gt 0) {
        $current = $queue.Dequeue()
        $sortedFiles += $fileGraph[$current].File
        
        foreach ($other in $fileGraph.Keys) {
            if ($fileGraph[$other].DependsOn -contains $current) {
                $inDegree[$other]--
                if ($inDegree[$other] -eq 0) {
                    $queue.Enqueue($other)
                }
            }
        }
    }
    
    # If circular dependency detected, use original order
    if ($sortedFiles.Count -ne $sqlFiles.Count) {
        Write-Host "  [WARN] Circular dependency detected, using alphabetical order" -ForegroundColor Yellow
        $sortedFiles = $sqlFiles | Sort-Object Name
    } else {
        Write-Host "  [INFO] Dependencies analyzed, importing in optimal order"
    }

    $importSuccess = $true
    foreach ($file in $sortedFiles) {
        Write-Host "  [IMPORT] File: $($file.Name)"
        psql -U $PGUser -h $PGHost -p $PGPort -d $dbName -v ON_ERROR_STOP=1 -f $file.FullName 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Import failed for: $($file.FullName)"
            $importSuccess = $false
            break
        }
    }

    if ($importSuccess) {
        Write-Host "  [DONE] Template processed successfully." -ForegroundColor Green
    } else {
        Write-Host "  [FAILED] Template processing incomplete." -ForegroundColor Red
    }
}

Write-Host "`n[SUCCESS] All templates imported!" -ForegroundColor Cyan