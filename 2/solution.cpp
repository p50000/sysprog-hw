#include "parser.h"

#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/fcntl.h>

static bool is_builtin(const command& cmd) {
	return cmd.exe == "cd" || cmd.exe == "exit";
}

static bool has_redirect(const struct command_line *line) {
	return line->out_type != OUTPUT_TYPE_STDOUT;
}

static bool is_logical_expression(const command_line *line) {
	for (auto& expr : line->exprs) {
		if (expr.type == EXPR_TYPE_AND || expr.type == EXPR_TYPE_OR) {
			return true;
		}
	}
	return false;
}

static int execute_builtin_command(const struct command& cmd, bool has_redirect) {
    if (cmd.exe == "cd") {
        const char* path;
		if (cmd.args.empty()) {
			path = getenv("HOME");
		} else {
			path = cmd.args[0].c_str();
		}
        if (path == nullptr) {
			path = "/";
		}

        if (chdir(path) != 0) {
			return 1;
        }
		return 0;
	}
    if (cmd.exe == "exit") {
		int exit_code = cmd.args.empty() ? 0 : std::stoi(cmd.args[0]);
		if (!has_redirect) {
			exit(exit_code);
		}
		return exit_code;
    }
	return 0;
}

static int execute_with_execv(const struct command& cmd) {
	std::vector<char*> argv;
	argv.push_back(const_cast<char*>(cmd.exe.c_str()));
	for (const std::string& arg : cmd.args) {
		argv.push_back(const_cast<char*>(arg.c_str()));
	}
	argv.push_back(nullptr);
	execvp(cmd.exe.c_str(), argv.data());
	// we exit, otherwise cmd not found
	return 127;
}

static int execute_pipeline(const std::list<expr>& exprs, bool has_redirect) {
	std::vector<std::array<int, 2>> pipes;
    std::vector<pid_t> pids;
    int prev_read_fd = STDIN_FILENO;
	bool is_piped = false;

	auto current = exprs.begin();
	while (current != exprs.end()) {
		// if we enter this code, we ecndounter only pipes or exprs
		assert (current->type == EXPR_TYPE_COMMAND && "not a command");

		auto next = std::next(current);
		int next_read_fd = STDIN_FILENO;
		int output_fd = STDOUT_FILENO;
		
		if (next != exprs.end() && next->type == EXPR_TYPE_PIPE) {
			is_piped = true;
			int pipefd[2];
			if (pipe(pipefd) == -1) {
				for (size_t i = 0; i < pipes.size(); i++) {
					close(pipes[i][0]);
					close(pipes[i][1]);
				}
				return -1;
			}
			pipes.push_back({pipefd[0], pipefd[1]});
			output_fd = pipefd[1];
			next_read_fd = pipefd[0];
			next = std::next(next);
		}
		assert(current->cmd.has_value());
		const command& cmd = current->cmd.value();

		if (is_builtin(cmd)) {
			execute_builtin_command(cmd, has_redirect || is_piped);
		}

		pid_t pid = fork();
		if (pid == 0) {
			// child
			if (prev_read_fd != STDIN_FILENO) {
				dup2(prev_read_fd, STDIN_FILENO);
			}
			if (output_fd != STDOUT_FILENO) {
				dup2(output_fd, STDOUT_FILENO);
			}
			
			for (size_t i = 0; i < pipes.size(); i++) {
				close(pipes[i][0]);
				close(pipes[i][1]);
			}
			
			if (is_builtin(cmd)) {
				int status = execute_builtin_command(cmd, has_redirect || is_piped);
				_exit(status);
			} else {
				int status = execute_with_execv(cmd);
				_exit(status);
			}
		} else if (pid > 0) {
			// parent
			pids.push_back(pid);
		} else {
			for (size_t i = 0; i < pipes.size(); i++) {
				close(pipes[i][0]);
				close(pipes[i][1]);
			}
			return -1;
		}

		// close used descriptors in parent
		if (prev_read_fd != STDIN_FILENO) {
			close(prev_read_fd);
		}
		if (output_fd != STDOUT_FILENO) {
			close(output_fd);
		}
		current = next;
		prev_read_fd = next_read_fd;
	}

	int last_status = 0;
    for (pid_t pid : pids) {
		int status;
		while (waitpid(pid, &status, 0) == -1) {
			if (errno == EINTR) continue;
			status = 127 << 8;
			break;
		}

        if (WIFEXITED(status)) {
            last_status = WEXITSTATUS(status);
        } else {
            last_status = 127;
        }
    }
    
    return last_status;
}

static int execute_logical_commands(const std::list<expr>& exprs, bool has_redirect) {
	std::vector<std::list<expr>> and_exprs;
    std::list<expr> current_segment;
    
    for (const auto& expr : exprs) {
        if (expr.type == EXPR_TYPE_AND) {
            if (!current_segment.empty()) {
                and_exprs.push_back(current_segment);
                current_segment.clear();
            }
        } else {
            current_segment.push_back(expr);
        }
    }
	if (!current_segment.empty()) {
        and_exprs.push_back(current_segment);
    }

	bool and_success = true;
	for (const auto& and_expr : and_exprs) {
		std::vector<std::list<expr>> or_exprs;
		std::list<expr> current_segment;
		for (const auto& expr : and_expr) {
			if (expr.type == EXPR_TYPE_OR) {
				if (!current_segment.empty()) {
					or_exprs.push_back(current_segment);
					current_segment.clear();
				}
			} else {
				current_segment.push_back(expr);
			}
		}
		if (!current_segment.empty()) {
			or_exprs.push_back(current_segment);
		}

		bool or_success = false;
		for (const auto& or_expr : or_exprs) {
			int status = execute_pipeline(or_expr, has_redirect);
			if (status == 0) {
				or_success = true;
				break;
			}
		}
		if (!or_success) {
			and_success = false;
			break;
		}
	}
	if (and_success) {
		return 0;
	} else {
		return 1;
	}
}

static int open_output_file(const std::string& filename, enum output_type type) {
    int flags = O_CREAT | O_WRONLY;
    int mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    
    if (type == OUTPUT_TYPE_FILE_NEW) {
        flags |= O_TRUNC;
    } else if (type == OUTPUT_TYPE_FILE_APPEND) {
        flags |= O_APPEND;
    }
    
    int fd = open(filename.c_str(), flags, mode);
    return fd;
}

static void reap_zombies_nonblocking() {
    int status;
    pid_t pid;

    while (true) {
        pid = waitpid(-1, &status, WNOHANG);
        if (pid > 0) {
            continue;
        }
        if (pid == 0) {
            break;
        }
        if (errno == EINTR) continue; // continue if interrupted
        break;
    }
}

static int execute_command_line_no_bg(const struct command_line *line) {
	int stdout = -1;
    if (line->out_type != OUTPUT_TYPE_STDOUT) {
        int output_fd = open_output_file(line->out_file, line->out_type);
        if (output_fd == -1) {
            return 1;
        }
        stdout = dup(STDOUT_FILENO);
        dup2(output_fd, STDOUT_FILENO);
        close(output_fd);
    }
    
	int status = 0;
	if (is_logical_expression(line)) {
    	status = execute_logical_commands(line->exprs, has_redirect(line));
	} else {
		status = execute_pipeline(line->exprs, has_redirect(line));
	}
    
    if (stdout != -1) {
        dup2(stdout, STDOUT_FILENO);
        close(stdout);
    }
	return status;
}

static int execute_command_line(const struct command_line *line) {
    assert(line != NULL);
    
    // Handle background process
    if (line->is_background) {
        pid_t pid = fork();
        if (pid == 0) {
            int status = execute_command_line_no_bg(line);
            _exit(status);
        } else if (pid > 0) {
            return 0;
        }
    }
    
    return execute_command_line_no_bg(line);
}

int main(void)
{
	const size_t buf_size = 1024;
	char buf[buf_size];
	int rc;
	struct parser *p = parser_new();

	int last_status = 0;
	while ((rc = read(STDIN_FILENO, buf, buf_size)) > 0) {
		parser_feed(p, buf, rc);
		struct command_line *line = NULL;
		while (true) {
			enum parser_error err = parser_pop_next(p, &line);
			if (err == PARSER_ERR_NONE && line == NULL)
				break;
			if (err != PARSER_ERR_NONE) {
				continue;
			}
			last_status = execute_command_line(line);
			delete line;
			reap_zombies_nonblocking();
		}
	}
	parser_delete(p);
	return last_status;
}
