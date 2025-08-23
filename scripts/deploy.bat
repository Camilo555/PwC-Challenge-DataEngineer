@echo off
REM =============================================================================
REM DEPLOYMENT SCRIPT FOR PWC RETAIL DATA PLATFORM (Windows)
REM =============================================================================

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."
set "ENVIRONMENTS=test dev prod"
set "DEFAULT_PROFILES=api,etl,orchestration"

REM Colors for output (Windows)
set "RED=[91m"
set "GREEN=[92m"
set "YELLOW=[93m"
set "BLUE=[94m"
set "NC=[0m"

REM Initialize variables
set "ENVIRONMENT="
set "PROFILES="
set "DETACH="
set "BUILD="
set "VERBOSE="
set "ACTION=deploy"
set "SERVICES="

REM Functions
:log_info
echo %BLUE%[INFO]%NC% %~1
goto :eof

:log_success
echo %GREEN%[SUCCESS]%NC% %~1
goto :eof

:log_warning
echo %YELLOW%[WARNING]%NC% %~1
goto :eof

:log_error
echo %RED%[ERROR]%NC% %~1
goto :eof

:usage
echo Usage: %~nx0 [OPTIONS] ENVIRONMENT
echo.
echo Deploy PWC Retail Data Platform to specified environment.
echo.
echo ENVIRONMENTS:
echo   test        Deploy to test environment (lightweight)
echo   dev         Deploy to development environment (with dev tools)
echo   prod        Deploy to production environment (full stack)
echo.
echo OPTIONS:
echo   /p PROFILES     Comma-separated list of profiles to deploy
echo   /d              Run in detached mode
echo   /b              Force rebuild of images
echo   /v              Verbose output
echo   /h              Show this help message
echo   /down           Stop and remove containers
echo   /logs           Show logs for running services
echo   /status         Show status of services
echo.
echo EXAMPLES:
echo   %~nx0 dev                      # Deploy dev environment
echo   %~nx0 prod /p api,monitoring   # Deploy prod with API and monitoring
echo   %~nx0 test /b /d               # Build and deploy test environment
echo   %~nx0 dev /down                # Stop dev environment
echo   %~nx0 prod /logs               # Show prod logs
echo.
goto :eof

:validate_environment
set "ENV=%~1"
for %%e in (%ENVIRONMENTS%) do (
    if "!ENV!"=="%%e" goto :validate_ok
)
call :log_error "Invalid environment: !ENV!"
call :log_info "Valid environments: %ENVIRONMENTS%"
exit /b 1
:validate_ok
goto :eof

:check_prerequisites
set "ENV=%~1"
call :log_info "Checking prerequisites for !ENV! environment..."

REM Check Docker and Docker Compose
where docker >nul 2>nul
if %errorlevel% neq 0 (
    call :log_error "Docker is not installed"
    exit /b 1
)

where docker-compose >nul 2>nul
if %errorlevel% neq 0 (
    call :log_error "Docker Compose is not installed"  
    exit /b 1
)

REM Check environment file
set "ENV_FILE=%PROJECT_DIR%\.env.!ENV!"
if not exist "!ENV_FILE!" (
    call :log_error "Environment file .env.!ENV! not found"
    exit /b 1
)

REM Check required directories
set "DATA_DIR=%PROJECT_DIR%\data\!ENV!"
set "LOGS_DIR=%PROJECT_DIR%\logs\!ENV!"

if not exist "!DATA_DIR!" mkdir "!DATA_DIR!"
if not exist "!LOGS_DIR!" mkdir "!LOGS_DIR!"

REM Environment-specific checks
if "!ENV!"=="prod" (
    if not exist "%PROJECT_DIR%\ssl\server.crt" (
        call :log_warning "SSL certificate not found - HTTPS will not work"
    )
    
    findstr "CHANGE_ME" "!ENV_FILE!" >nul 2>nul
    if !errorlevel! equ 0 (
        call :log_error "Production secrets not configured in .env.!ENV!"
        exit /b 1
    )
)

if "!ENV!"=="dev" (
    if not exist "%PROJECT_DIR%\notebooks" (
        mkdir "%PROJECT_DIR%\notebooks"
        call :log_info "Created notebooks directory for Jupyter"
    )
)

if "!ENV!"=="test" (
    if not exist "%PROJECT_DIR%\data\test" (
        mkdir "%PROJECT_DIR%\data\test"
        call :log_info "Created test data directory"
    )
)

call :log_success "Prerequisites check completed"
goto :eof

:deploy_environment
set "ENV=%~1"
set "PROF=%~2"
set "ARGS=%~3"

call :log_info "Deploying !ENV! environment with profiles: !PROF!"

cd /d "%PROJECT_DIR%"

REM Set environment variables
set "COMPOSE_FILE=docker-compose.base.yml;docker-compose.!ENV!.yml"
set "COMPOSE_PROFILES=!PROF!"

REM Build compose command
set "COMPOSE_CMD=docker-compose -f docker-compose.base.yml -f docker-compose.!ENV!.yml --env-file .env.!ENV!"

REM Add profiles
if not "!PROF!"=="" (
    for %%p in (!PROF:,= !) do (
        set "COMPOSE_CMD=!COMPOSE_CMD! --profile %%p"
    )
)

REM Execute command
call :log_info "Running: !COMPOSE_CMD! up !ARGS!"
!COMPOSE_CMD! up !ARGS!
goto :eof

:stop_environment
set "ENV=%~1"
call :log_info "Stopping !ENV! environment..."

cd /d "%PROJECT_DIR%"

docker-compose -f docker-compose.base.yml -f docker-compose.!ENV!.yml --env-file .env.!ENV! down --remove-orphans

call :log_success "!ENV! environment stopped"
goto :eof

:show_logs
set "ENV=%~1"
cd /d "%PROJECT_DIR%"

call :log_info "Showing logs for !ENV! environment:"
docker-compose -f docker-compose.base.yml -f docker-compose.!ENV!.yml --env-file .env.!ENV! logs -f
goto :eof

:show_status
set "ENV=%~1"
cd /d "%PROJECT_DIR%"

call :log_info "Status of !ENV! environment:"
docker-compose -f docker-compose.base.yml -f docker-compose.!ENV!.yml --env-file .env.!ENV! ps
goto :eof

REM Parse arguments
:parse_args
if "%~1"=="" goto :args_done
if "%~1"=="/p" (
    set "PROFILES=%~2"
    shift
    shift
    goto :parse_args
)
if "%~1"=="/d" (
    set "DETACH=-d"
    shift
    goto :parse_args
)
if "%~1"=="/b" (
    set "BUILD=--build"
    shift
    goto :parse_args
)
if "%~1"=="/v" (
    set "VERBOSE=--verbose"
    shift
    goto :parse_args
)
if "%~1"=="/down" (
    set "ACTION=down"
    shift
    goto :parse_args
)
if "%~1"=="/logs" (
    set "ACTION=logs"
    shift
    goto :parse_args
)
if "%~1"=="/status" (
    set "ACTION=status"
    shift
    goto :parse_args
)
if "%~1"=="/h" (
    call :usage
    exit /b 0
)
if "%~1"=="/?" (
    call :usage
    exit /b 0
)
if "%~1"=="-h" (
    call :usage
    exit /b 0
)
if "%~1"=="--help" (
    call :usage
    exit /b 0
)
if "!ENVIRONMENT!"=="" (
    set "ENVIRONMENT=%~1"
) else (
    set "SERVICES=!SERVICES! %~1"
)
shift
goto :parse_args

:args_done

REM Main execution
call :parse_args %*

REM Validate environment
if "!ENVIRONMENT!"=="" (
    call :log_error "Environment is required"
    call :usage
    exit /b 1
)

call :validate_environment "!ENVIRONMENT!"
if %errorlevel% neq 0 exit /b 1

REM Set default profiles
if "!PROFILES!"=="" (
    set "PROFILES=%DEFAULT_PROFILES%"
)

REM Build arguments
set "COMPOSE_ARGS=!DETACH! !BUILD! !VERBOSE!"

REM Execute action
if "!ACTION!"=="deploy" (
    call :check_prerequisites "!ENVIRONMENT!"
    if !errorlevel! neq 0 exit /b 1
    
    call :deploy_environment "!ENVIRONMENT!" "!PROFILES!" "!COMPOSE_ARGS!"
    call :log_success "!ENVIRONMENT! environment deployed successfully!"
)

if "!ACTION!"=="down" (
    call :stop_environment "!ENVIRONMENT!"
)

if "!ACTION!"=="logs" (
    call :show_logs "!ENVIRONMENT!"
)

if "!ACTION!"=="status" (
    call :show_status "!ENVIRONMENT!"
)

endlocal