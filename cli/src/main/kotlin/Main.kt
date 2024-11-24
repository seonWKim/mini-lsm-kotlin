package org.github.seonwkim

import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.ExperimentalCli
import kotlinx.cli.Subcommand
import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.storage.MiniLsm
import org.github.seonwkim.lsm.storage.NoCompaction
import java.nio.file.Paths

@OptIn(ExperimentalCli::class)
fun main() {
    val path = Paths.get("").toAbsolutePath().resolve("cli-output")
    val miniLsm = MiniLsm.open(
        path = path,
        options = LsmStorageOptions(
            blockSize = 4096,
            targetSstSize = 2 shl 20,
            numMemTableLimit = 10,
            enableWal = false,
            compactionOptions = NoCompaction,
        )
    )

    class GetCommand : Subcommand("get", "Get command") {
        val key by argument(ArgType.String, "key", description = "key")
        override fun execute() {
            println(miniLsm.get(key))
        }
    }

    class PutCommand : Subcommand("put", "Put command") {
        val key by argument(ArgType.String, "key", "key")
        val value by argument(ArgType.String, "value", "value")
        override fun execute() {
            miniLsm.put(key, value)
        }
    }

    class DeleteCommand : Subcommand("delete", "Delete command") {
        val key by argument(ArgType.String, "key", "key")
        override fun execute() {
            miniLsm.delete(key)
        }
    }

    class FreezeCommand : Subcommand("freeze", "Freeze memTable") {
        override fun execute() {
            miniLsm.inner.state.withWriteLock {
                miniLsm.inner.forceFreezeMemTable(it)
            }
        }

    }

    class FlushCommand : Subcommand("flush", "Flush memTable") {
        override fun execute() {
            miniLsm.inner.state.withWriteLock {
                miniLsm.inner.forceFlushNextImmMemTable()
            }
        }
    }

    while (true) {
        val parser = ArgParser("lsm")
        val getCommand = GetCommand()
        val putCommand = PutCommand()
        val deleteCommand = DeleteCommand()
        val freezeCommand = FreezeCommand()
        val flushCommand = FlushCommand()
        parser.subcommands(
            getCommand,
            putCommand,
            deleteCommand,
            freezeCommand,
            flushCommand
        )

        print("Enter command: ")
        try {
            val input = readLine() ?: break
            val inputArgs = input.split(" ").filter { it.isNotEmpty() }.toTypedArray()
            parser.parse(inputArgs)
        } catch (e: Exception) {
            println("Error: ${e.message}")
        }
    }
}
