async function findRootDir(): Promise<string> {
	let currentDir = Deno.cwd();
	while (true) {
		try {
			const cargoLockPath = `${currentDir}/Cargo.lock`;
			await Deno.stat(cargoLockPath);
			return currentDir;
		} catch {
			const parentDir = currentDir.substring(0, currentDir.lastIndexOf("/"));
			if (parentDir === currentDir) {
				throw new Error("Root dir not found");
			}
			currentDir = parentDir;
		}
	}
}

export const rootDir = await findRootDir();
