
public class Driver {
	public static void main(String[] args) throws Exception {
		
		DataDividerByUser dataDividerByUser = new DataDividerByUser();
		//MovieList movieList = new MovieList();
        EstimateRating estimateRating = new EstimateRating();
		CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
		Normalize normalize = new Normalize();
		Multiplication multiplication = new Multiplication();
		Sum sum = new Sum();

		//  /input/userRating.txt /input /dataDividedByUser /EstimateRating /coOccurrenceMatrix /Normalize /Multiplication /Sum

		String rawInputFile = args[0];
		String rawInput = args[1];
		String userMovieListOutputDir = args[2];
		String estimateUserMovieListOutputDir = args[3];
		String coOccurrenceMatrixDir = args[4];
		String normalizeDir = args[5];
		String multiplicationDir = args[6];
		String sumDir = args[7];

		String[] path1 = {rawInput, userMovieListOutputDir};
		String[] path2 = {rawInputFile, rawInput, estimateUserMovieListOutputDir};
		String[] path3 = {userMovieListOutputDir, coOccurrenceMatrixDir};
		String[] path4 = {coOccurrenceMatrixDir, normalizeDir};
		String[] path5 = {normalizeDir, rawInput, estimateUserMovieListOutputDir, multiplicationDir};
		String[] path6 = {multiplicationDir, sumDir};

		dataDividerByUser.main(path1);
        estimateRating.main(path2);
		coOccurrenceMatrixGenerator.main(path3);
		normalize.main(path4);
		multiplication.main(path5);
		sum.main(path6);

	}

}
