import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

def BLOCK_DELIMITER = "----"
def STATEMENT_CONTINUATION_CHARACTERS = [".", ",", "{", "("]
def STATEMENT_PREFIX = "gremlin> "
def STATEMENT_CONTINUATION_PREFIX = "         "

def header = """
    import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
    import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.SocialTraversal
    import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

    import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__
    import static com.tinkerpop.gremlin.process.T.*
    import static com.tinkerpop.gremlin.structure.Compare.*
    import static com.tinkerpop.gremlin.structure.Contains.*
    import static com.tinkerpop.gremlin.structure.Operator.*
    import static com.tinkerpop.gremlin.structure.Order.*
    import static java.util.Comparator.*
"""

def skipNextRead = false
def inCodeSection = false
def shell

sanitize = { def codeLine ->
    codeLine.replaceAll(/\s*(\<\d+\>,\s*)*\<\d+\>\s*$/, "").replaceAll(/\s*\/\/.*$/, "").trim()
}

new File(this.args[0]).withReader { reader ->
    while (skipNextRead || (line = reader.readLine()) != null) {
        skipNextRead = false
        if (inCodeSection) {
            inCodeSection = !line.equals(BLOCK_DELIMITER)
            if (inCodeSection) {
                def script = new StringBuilder(header)
                def sanitizedLine = sanitize(line)
                script.append(sanitizedLine)
                println STATEMENT_PREFIX + line
                if (!sanitizedLine.isEmpty() && sanitizedLine[-1] in STATEMENT_CONTINUATION_CHARACTERS) {
                    while (true) {
                        line = reader.readLine()
                        if (!line.startsWith(" ") && !line.startsWith("}") && !line.startsWith(")")) {
                            skipNextRead = true
                            break
                        }
                        sanitizedLine = sanitize(line)
                        script.append(sanitizedLine)
                        println STATEMENT_CONTINUATION_PREFIX + line
                    }
                }
                def res = shell.evaluate(script.toString())
                if (line.startsWith("import")) {
                    println "..."
                } else {
                    if (res instanceof Traversal) {
                        while (res.hasNext()) {
                            def current = res.next()
                            println "==>" + current
                        }
                    } else {
                        println "==>" + (res ?: "null")
                    }
                }
                if (line.equals(BLOCK_DELIMITER)) {
                    skipNextRead = false
                    inCodeSection = false
                }
            }
            if (!inCodeSection) println BLOCK_DELIMITER
        } else {
            if (line.startsWith("[gremlin")) {
                def parts = line.split(/,/, 2)
                def graph = parts.size() == 2 ? parts[1].capitalize().replaceAll(/\s*\]\s*$/, "") : ""
                def binding = new Binding()
                binding.setProperty("g", graph.isEmpty() ? TinkerGraph.open() : TinkerFactory."create${graph}"())
                shell = new GroovyShell(this.class.classLoader, binding)
                reader.readLine()
                inCodeSection = true
                println "[source,groovy]"
                println BLOCK_DELIMITER
            } else println line
        }
    }
}
